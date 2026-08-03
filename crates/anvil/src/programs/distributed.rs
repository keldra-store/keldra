use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anvil_atomic_program::{
    AtomicProgramEngine, ExecutionLease, InvocationContext, ObjectPath, ProgramInvocation,
    ProgramSnapshot, StateReader, StoredValue, VersionedDocument,
};
use anvil_consensus::{
    ATOMIC_REPLAY_RETENTION_MILLIS, BundleHash, BundleRef, Command, CommitBatch,
    CommittedInvocation, DurabilityClass, DurabilityEvidenceHash, ExecutorNomination,
    InvocationFingerprint, NodeId, ProgramHash as DecisionProgramHash, ProgramPathHash,
};
use anvil_store::{
    BlobRef, PreparedBundleHash, PreparedBundleRef, PreparedProgramBundle, PreparedProgramRecord,
    ProgramHash, ProgramPathMutation, ProgramPathStage, Store, Version, path_stage_from_prepared,
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::ClusterPeerTransport;
use crate::logical_name_resolution::LogicalNameResolver;
use crate::object_distribution::ObjectDistribution;

impl super::ProgramCoordinator {
    pub(crate) fn is_clustered(&self) -> Result<bool, Status> {
        let distributed = self.distributed()?;
        Ok(!distributed.objects.is_single_node()?)
    }

    pub(crate) fn executor_routing_target(&self) -> Result<Option<(NodeId, String)>, Status> {
        let nomination = self
            .decisions
            .state()
            .map_err(super::decision_status)?
            .executor()
            .ok_or_else(|| Status::unavailable("EXECUTOR_MOVED: no executor is nominated"))?;
        if nomination.executor == self.node {
            return Ok(None);
        }
        let placement = self.distributed()?.objects.current_program_placement()?;
        let address = placement.address(nomination.executor).ok_or_else(|| {
            Status::unavailable(format!(
                "nominated atomic executor {} has no ACTIVE peer address",
                nomination.executor.0
            ))
        })?;
        Ok(Some((nomination.executor, address.0.clone())))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invoke_distributed<F, Fut>(
        &self,
        program_key: anvil_store::ObjectKey,
        expected_program_hash: [u8; 32],
        invocation_id: String,
        input_json: &[u8],
        durability_class: &str,
        budget: Duration,
        authorize: F,
    ) -> Result<super::InvokedProgramResult, Status>
    where
        F: Fn(Vec<anvil_atomic_program::ExpandedProgramPath>) -> Fut,
        Fut: std::future::Future<Output = Result<(), Status>>,
    {
        super::validate_program_request(
            &program_key,
            &invocation_id,
            input_json,
            durability_class,
        )?;
        let nomination = self.current_nomination()?;
        let distributed = self.distributed()?.clone();
        let expected_hash = ProgramHash(expected_program_hash);
        let program = distributed
            .load_program(&program_key, expected_hash)
            .await?;
        let context =
            InvocationContext::new(program_key.tenant()).map_err(Status::invalid_argument)?;
        let input = if input_json.is_empty() {
            anvil_atomic_program::ProgramInput::default()
        } else {
            serde_json::from_slice(input_json).map_err(|error| {
                Status::invalid_argument(format!("invalid program input JSON: {error}"))
            })?
        };
        let program_path_hash = super::program_path_hash(&program_key);
        let invocation = ProgramInvocation::from_input(program_path_hash, invocation_id, input)
            .map_err(Status::invalid_argument)?;
        let fingerprint = super::decode_fingerprint(&invocation.input_fingerprint)?;
        let consensus_invocation_id =
            super::invocation_identity(&program_key, &invocation.command_id);
        let evaluator = distributed.evaluator(&program)?;

        let _gate = self.commit_gate.lock().await;
        self.recover_distributed_tail_locked().await?;
        let expanded = evaluator.expanded_paths(&context, &invocation)?;
        authorize(expanded).await?;
        let replay_clock = super::current_unix_millis().map_err(super::internal)?;
        if let Some(committed) = self
            .decisions
            .state()
            .map_err(super::decision_status)?
            .replay_entry(consensus_invocation_id, replay_clock)
        {
            super::require_same_invocation(
                committed,
                fingerprint,
                program_path_hash,
                expected_program_hash,
            )?;
            return self.load_distributed_result(committed, true).await;
        }

        let prepared = distributed
            .prepare_and_stage(
                &evaluator,
                &context,
                &invocation,
                durability_class,
                nomination,
                budget,
            )
            .await?;
        let current = self.current_nomination()?;
        if current != nomination {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: atomic executor changed during preparation",
            ));
        }
        let proposal_at_unix_millis = super::current_unix_millis().map_err(super::internal)?;
        let replay_expires_at_unix_millis = proposal_at_unix_millis
            .checked_add(ATOMIC_REPLAY_RETENTION_MILLIS)
            .ok_or_else(|| Status::internal("atomic replay expiry overflow"))?;
        let evidence_hash = super::accepted_program_evidence_hash(
            &prepared.prepared.durability.scope,
            prepared.prepared.durability_evidence_hash,
            durability_class,
            self.node,
        )?;
        let committed = self
            .decisions
            .submit(Command::CommitBatch(CommitBatch {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                program_path_hash: ProgramPathHash(program_path_hash),
                program_hash: DecisionProgramHash(expected_program_hash),
                invocation_id: consensus_invocation_id,
                input_fingerprint: InvocationFingerprint(fingerprint),
                bundle_ref: BundleRef {
                    hash: prepared.prepared.bundle.hash,
                    length: prepared.prepared.bundle.length,
                },
                bundle_hash: BundleHash(prepared.prepared.hash.0),
                durability_class: DurabilityClass(
                    anvil_store::ProgramDurabilityClassHash::for_class(durability_class).0,
                ),
                durability_evidence_hash: DurabilityEvidenceHash(evidence_hash.0),
                proposal_at_unix_millis,
                replay_expires_at_unix_millis,
            }))
            .await
            .map_err(super::decision_status)?;
        let committed = super::expect_batch_committed(committed.result)?;
        if committed.replayed {
            drop(prepared.lease);
            return self
                .load_distributed_result(committed.invocation, true)
                .await;
        }
        distributed
            .finalize(
                &prepared.stages,
                committed.invocation.committed_batch.commit_cursor,
                nomination,
                budget,
            )
            .await?;
        self.advance_finalized_through(
            nomination,
            committed.invocation.committed_batch.commit_cursor,
        )
        .await?;
        drop(prepared.lease);
        Ok(result_from_record(
            &prepared.record,
            committed.invocation,
            false,
        ))
    }

    pub(super) async fn recover_distributed_tail_locked(&self) -> Result<(), Status> {
        let nomination = self.current_nomination()?;
        let distributed = self.distributed()?.clone();
        let state = self.decisions.state().map_err(super::decision_status)?;
        let invocations = state.unfinalized_invocations().collect::<Vec<_>>();
        drop(state);
        let mut through = None;
        for invocation in invocations {
            let (_record, stages) = distributed.recover_record(invocation).await?;
            distributed
                .finalize(
                    &stages,
                    invocation.committed_batch.commit_cursor,
                    nomination,
                    Duration::from_secs(30),
                )
                .await?;
            through = Some(invocation.committed_batch.commit_cursor);
        }
        if let Some(through) = through {
            self.advance_finalized_through(nomination, through).await?;
        }
        Ok(())
    }

    async fn load_distributed_result(
        &self,
        invocation: CommittedInvocation,
        replayed: bool,
    ) -> Result<super::InvokedProgramResult, Status> {
        let (record, _) = self.distributed()?.recover_record(invocation).await?;
        Ok(result_from_record(&record, invocation, replayed))
    }

    fn distributed(&self) -> Result<&DistributedPrograms, Status> {
        self.distributed
            .get()
            .ok_or_else(|| Status::unavailable("distributed atomic programs are not ready"))
    }
}

#[derive(Clone)]
pub(super) struct DistributedPrograms {
    local_node: NodeId,
    store: Store,
    reader: ClusterObjectReader,
    objects: ObjectDistribution,
    peers: ClusterPeerTransport,
    names: LogicalNameResolver,
}

pub(super) struct PreparedDistributedInvocation {
    pub(super) lease: ExecutionLease,
    pub(super) prepared: PreparedProgramBundle,
    pub(super) record: PreparedProgramRecord,
    pub(super) stages: Vec<ProgramPathStage>,
}

impl DistributedPrograms {
    pub(super) fn new(
        local_node: NodeId,
        store: Store,
        reader: ClusterObjectReader,
        objects: ObjectDistribution,
        peers: ClusterPeerTransport,
        names: LogicalNameResolver,
    ) -> Self {
        Self {
            local_node,
            store,
            reader,
            objects,
            peers,
            names,
        }
    }

    pub(super) async fn load_program(
        &self,
        key: &anvil_store::ObjectKey,
        expected_hash: ProgramHash,
    ) -> Result<anvil_store::VerifiedProgramDefinition, Status> {
        let object = self
            .reader
            .open(key, None)
            .await?
            .ok_or_else(|| Status::not_found("program definition object was not found"))?;
        if object.version.deleted {
            return Err(Status::not_found("program definition object was deleted"));
        }
        let mut payload = object
            .payload
            .ok_or_else(|| Status::data_loss("program definition has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read program definition: {error}")))?;
        anvil_store::VerifiedProgramDefinition::from_bytes(&bytes, expected_hash)
            .map_err(super::program_store_status)
    }

    pub(super) fn evaluator(
        &self,
        definition: &anvil_store::VerifiedProgramDefinition,
    ) -> Result<DistributedEvaluator, Status> {
        let snapshots = Arc::new(Mutex::new(None));
        let reader = DistributedStateReader {
            reader: self.reader.clone(),
            snapshots: snapshots.clone(),
        };
        let engine = AtomicProgramEngine::with_lock_manager(
            definition.definition.clone(),
            reader,
            self.store.lock_manager(),
        )
        .map_err(super::engine_status)?;
        Ok(DistributedEvaluator {
            program_hash: definition.hash,
            engine,
            snapshots,
        })
    }

    pub(super) async fn prepare_and_stage(
        &self,
        evaluator: &DistributedEvaluator,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
        durability_class: &str,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<PreparedDistributedInvocation, Status> {
        let lease = evaluator
            .engine
            .prepare(context, invocation)
            .await
            .map_err(super::engine_status)?;
        let previous = evaluator.previous_versions()?;
        let mut prepared = self
            .store
            .prepare_distributed_program_bundle(evaluator.program_hash, lease.bundle(), &previous)
            .await
            .map_err(super::program_store_status)?;
        let record = self
            .store
            .prepared_program_record(&prepared)
            .await
            .map_err(super::program_store_status)?;

        for write in record.writes() {
            if let Some(reference) = write.version().blob.as_ref() {
                self.objects.prepare_program_blob(reference).await?;
            }
        }
        self.objects
            .prepare_program_blob(&BlobRef::from(prepared.bundle))
            .await?;
        prepared
            .attest_remote_durability(durability_class)
            .map_err(super::program_store_status)?;

        let mut stages = Vec::with_capacity(record.writes().len());
        for write in record.writes() {
            let (tenant_id, bucket_id) = self
                .names
                .resolve_bucket_ids(&write.path().tenant, &write.path().bucket)
                .await?;
            stages.push(
                path_stage_from_prepared(&prepared, write, tenant_id, bucket_id)
                    .map_err(super::program_store_status)?,
            );
        }
        for stage in &stages {
            self.stage_path(stage, nomination, budget).await?;
        }
        Ok(PreparedDistributedInvocation {
            lease,
            prepared,
            record,
            stages,
        })
    }

    async fn stage_path(
        &self,
        stage: &ProgramPathStage,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        let placement = self.objects.current_program_placement()?;
        let group = self.objects.program_replica_group(
            stage.tenant_id,
            stage.bucket_id,
            &stage.path.path,
        )?;
        let expected = stage.blob_ref().map_err(super::program_store_status)?;
        let mut durable = Vec::new();
        for node in group.replicas().iter().copied() {
            let result = if node == self.local_node {
                self.store
                    .persist_program_path_stage(stage)
                    .await
                    .map_err(super::program_store_status)
            } else {
                let address = placement.address(node).ok_or_else(|| {
                    Status::unavailable(format!("ACTIVE node {} has no peer address", node.0))
                })?;
                self.peers
                    .stage_program_path(
                        node,
                        &address.0,
                        nomination.nomination_log_index,
                        stage,
                        budget,
                    )
                    .await
            };
            match result {
                Ok(reference) if reference == expected => durable.push(node),
                Ok(_) => tracing::warn!(node_id = node.0, "program stage identity mismatch"),
                Err(error) => {
                    tracing::warn!(node_id = node.0, %error, "program path stage failed")
                }
            }
        }
        if !group.is_acknowledged_by(&durable) {
            return Err(Status::unavailable(format!(
                "program path stage reached {} of {} required replicas",
                durable.len(),
                group.required_acknowledgements()
            )));
        }
        if self.objects.current_program_placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "placement changed during program path staging",
            ));
        }
        Ok(())
    }

    pub(super) async fn finalize(
        &self,
        stages: &[ProgramPathStage],
        commit_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        for stage in stages {
            self.finalize_path(stage, commit_cursor, nomination, budget)
                .await?;
        }
        Ok(())
    }

    async fn finalize_path(
        &self,
        stage: &ProgramPathStage,
        commit_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        let placement = self.objects.current_program_placement()?;
        let group = self.objects.program_replica_group(
            stage.tenant_id,
            stage.bucket_id,
            &stage.path.path,
        )?;
        let coordinator = group.coordinator();
        let mutation = if coordinator == self.local_node {
            self.store
                .coordinate_program_path_finalization(
                    stage.clone(),
                    commit_cursor,
                    self.objects.program_mutation_context()?,
                )
                .await
                .map_err(super::program_store_status)?
                .mutation
        } else {
            let address = placement.address(coordinator).ok_or_else(|| {
                Status::unavailable(format!(
                    "ACTIVE path authority {} has no peer address",
                    coordinator.0
                ))
            })?;
            self.peers
                .coordinate_program_path_finalization(
                    coordinator,
                    &address.0,
                    nomination.nomination_log_index,
                    commit_cursor,
                    stage,
                    budget,
                )
                .await?
        };
        require_mutation(&mutation, stage, commit_cursor, coordinator)?;
        let mut durable = vec![coordinator];
        for replica in group
            .replicas()
            .iter()
            .copied()
            .filter(|replica| *replica != coordinator)
        {
            let result = if replica == self.local_node {
                self.store
                    .apply_program_path_finalization_replica(&mutation)
                    .await
                    .map_err(super::program_store_status)
            } else {
                let address = placement.address(replica).ok_or_else(|| {
                    Status::unavailable(format!(
                        "ACTIVE path replica {} has no peer address",
                        replica.0
                    ))
                })?;
                self.peers
                    .apply_program_path_finalization(
                        replica,
                        &address.0,
                        nomination.nomination_log_index,
                        &mutation,
                        budget,
                    )
                    .await
            };
            match result {
                Ok(applied) if applied.version == stage.version.id => durable.push(replica),
                Ok(_) => tracing::warn!(node_id = replica.0, "program replica version mismatch"),
                Err(error) => {
                    tracing::warn!(node_id = replica.0, %error, "program replica finalize failed")
                }
            }
        }
        if !group.is_acknowledged_by(&durable) {
            return Err(Status::unavailable(format!(
                "program finalization reached {} of {} required replicas",
                durable.len(),
                group.required_acknowledgements()
            )));
        }
        if self.objects.current_program_placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "placement changed during program path finalization",
            ));
        }
        Ok(())
    }

    pub(super) async fn recover_record(
        &self,
        invocation: CommittedInvocation,
    ) -> Result<(PreparedProgramRecord, Vec<ProgramPathStage>), Status> {
        let batch = invocation.committed_batch;
        let bundle = PreparedBundleRef {
            hash: batch.bundle_ref.hash,
            length: batch.bundle_ref.length,
        };
        let bytes = self.reader.read_blob_bytes(&BlobRef::from(bundle)).await?;
        let record = PreparedProgramRecord::decode_distributed(
            &bytes,
            bundle,
            PreparedBundleHash(batch.bundle_hash.0),
            ProgramHash(batch.program_hash.0),
        )
        .map_err(super::program_store_status)?;
        let prepared = PreparedProgramBundle {
            hash: PreparedBundleHash(batch.bundle_hash.0),
            source_bundle_hash: record.source_bundle_hash(),
            program_hash: ProgramHash(batch.program_hash.0),
            bundle,
            durability_evidence_hash: anvil_store::ProgramDurabilityEvidenceHash(
                batch.durability_evidence_hash.0,
            ),
            durability: anvil_store::ProgramDurabilityEvidence {
                format: 1,
                bundle,
                scope: anvil_store::ProgramDurabilityScope::ConfiguredRemote {
                    class: "recovery".into(),
                },
                provider_receipt: Vec::new(),
            },
        };
        let mut stages = Vec::with_capacity(record.writes().len());
        for write in record.writes() {
            let (tenant_id, bucket_id) = self
                .names
                .resolve_bucket_ids(&write.path().tenant, &write.path().bucket)
                .await?;
            stages.push(
                path_stage_from_prepared(&prepared, write, tenant_id, bucket_id)
                    .map_err(super::program_store_status)?,
            );
        }
        Ok((record, stages))
    }
}

pub(super) struct DistributedEvaluator {
    program_hash: ProgramHash,
    engine: AtomicProgramEngine<DistributedStateReader>,
    snapshots: Arc<Mutex<Option<DistributedSnapshot>>>,
}

impl DistributedEvaluator {
    pub(super) fn expanded_paths(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
    ) -> Result<Vec<anvil_atomic_program::ExpandedProgramPath>, Status> {
        self.engine
            .expanded_paths(context, invocation)
            .map_err(super::engine_status)
    }

    fn previous_versions(&self) -> Result<BTreeMap<ObjectPath, Version>, Status> {
        self.snapshots
            .lock()
            .map_err(|_| Status::internal("distributed program snapshot lock is poisoned"))?
            .as_ref()
            .map(|snapshot| snapshot.versions.clone())
            .ok_or_else(|| Status::internal("distributed evaluator did not retain its snapshot"))
    }
}

#[derive(Clone)]
struct DistributedStateReader {
    reader: ClusterObjectReader,
    snapshots: Arc<Mutex<Option<DistributedSnapshot>>>,
}

#[derive(Clone)]
struct DistributedSnapshot {
    versions: BTreeMap<ObjectPath, Version>,
}

impl StateReader for DistributedStateReader {
    async fn read_snapshot(&self, paths: &[ObjectPath]) -> Result<ProgramSnapshot, String> {
        let mut documents = BTreeMap::new();
        let mut versions = BTreeMap::new();
        for path in paths {
            let key = anvil_store::ObjectKey::new(&path.tenant, &path.bucket, &path.path)
                .map_err(|error| error.to_string())?;
            let Some(mut object) = self
                .reader
                .open(&key, None)
                .await
                .map_err(|error| error.to_string())?
            else {
                continue;
            };
            let value = if object.version.deleted {
                None
            } else {
                let mut payload = object
                    .payload
                    .take()
                    .ok_or_else(|| "live program dependency has no payload".to_owned())?;
                let mut bytes = Vec::new();
                payload
                    .read_to_end(&mut bytes)
                    .map_err(|error| error.to_string())?;
                let content_type = object
                    .version
                    .content_type
                    .as_deref()
                    .unwrap_or("application/octet-stream");
                Some(
                    if content_type
                        .split(';')
                        .next()
                        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
                    {
                        StoredValue::Json(
                            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
                        )
                    } else {
                        StoredValue::Opaque(bytes)
                    },
                )
            };
            documents.insert(
                path.clone(),
                VersionedDocument {
                    version: object.version.id.0.to_string(),
                    value,
                    content_type: object.version.content_type.clone(),
                },
            );
            versions.insert(path.clone(), object.version);
        }
        *self
            .snapshots
            .lock()
            .map_err(|_| "distributed program snapshot lock is poisoned".to_owned())? =
            Some(DistributedSnapshot { versions });
        Ok(ProgramSnapshot { documents })
    }
}

fn require_mutation(
    mutation: &ProgramPathMutation,
    stage: &ProgramPathStage,
    commit_cursor: u64,
    coordinator: NodeId,
) -> Result<(), Status> {
    mutation.validate().map_err(super::program_store_status)?;
    if mutation.stage != *stage
        || mutation.commit_cursor != commit_cursor
        || u64::from(mutation.stamp.source_id.node_id) != coordinator.0
    {
        return Err(Status::data_loss(
            "path authority returned another program finalization",
        ));
    }
    Ok(())
}

fn result_from_record(
    record: &PreparedProgramRecord,
    invocation: CommittedInvocation,
    replayed: bool,
) -> super::InvokedProgramResult {
    let published_versions = record
        .writes()
        .iter()
        .map(|write| {
            (
                write.path().clone(),
                anvil_store::PublishedProgramVersion {
                    version: write.version().id,
                    deleted: write.version().deleted,
                },
            )
        })
        .collect();
    super::InvokedProgramResult {
        receipt: record.receipt().clone(),
        executor_nomination_log_index: invocation.committed_batch.nomination_log_index,
        commit_log_index: invocation.committed_batch.commit_cursor,
        program_hash: invocation.committed_batch.program_hash.0,
        published_versions,
        replayed,
        replay_guarantee_expires_at_unix_millis: invocation.replay_expires_at_unix_millis,
    }
}
