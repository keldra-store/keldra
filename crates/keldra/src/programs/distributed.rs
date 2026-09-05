use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keldra_atomic_program::{
    AtomicProgramEngine, ExecutionLease, InvocationContext, ObjectPath, ProgramInvocation,
    ProgramSnapshot, StateReader, StoredValue, VersionedDocument,
};
use keldra_consensus::{
    ATOMIC_REPLAY_RETENTION_MILLIS, AtomicBundleAuthority, BeginBatch, BeginResult, BundleHash,
    BundleRef, Command, CommitPreparedBatch, CommittedInvocation, DurabilityClass,
    DurabilityEvidenceHash, ExecutorNomination, InvocationFingerprint, NodeId,
    ParticipantManifestHash, PreparedBatch,
};
use keldra_store::{
    BlobRef, BuiltInObjectTransactionPlan, ObjectMutationContext, PlacementLogId,
    PreparedBundleHash, PreparedBundleRef, PreparedProgramBundle, PreparedProgramRecord,
    ProgramAliasBinding, ProgramAliasRegistryMutation, ProgramAliasRegistryStage, ProgramHash,
    ProgramPathMutation, ProgramPathStage, ProgramReservation, SealedAtomicBatchPublication, Store,
    Version, alias_registry_stages_from_prepared, path_stage_from_prepared,
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
        self.executor_replay_routing_target()
            .map(|target| target.map(|(node, address, _)| (node, address)))
    }

    pub(crate) fn executor_replay_routing_target(
        &self,
    ) -> Result<Option<(NodeId, String, u64)>, Status> {
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
        Ok(Some((
            nomination.executor,
            address.0.clone(),
            nomination.nomination_log_index,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invoke_distributed<L, LFut, C, CFut>(
        &self,
        program_key: keldra_store::ObjectKey,
        expected_program_hash: [u8; 32],
        invocation_id: String,
        input_json: &[u8],
        durability_class: &str,
        budget: Duration,
        authorize_logical: L,
        authorize_canonical: C,
    ) -> Result<super::InvokedProgramResult, Status>
    where
        L: Fn(Vec<keldra_atomic_program::ExpandedProgramPath>) -> LFut,
        LFut: std::future::Future<Output = Result<(), Status>>,
        C: Fn(Vec<keldra_atomic_program::ExpandedProgramPath>) -> CFut,
        CFut: std::future::Future<Output = Result<(), Status>>,
    {
        super::validate_program_request(
            &program_key,
            &invocation_id,
            input_json,
            durability_class,
            super::ProgramRuntimeTopology::Clustered,
        )?;
        self.require_generalized_atomic_paths()?;
        let nomination = self.current_nomination()?;
        let distributed = self.distributed()?.clone();
        let expected_hash = ProgramHash(expected_program_hash);
        let program = distributed
            .load_program(&program_key, expected_hash)
            .await?;
        let context =
            InvocationContext::new(program_key.tenant()).map_err(Status::invalid_argument)?;
        let input = if input_json.is_empty() {
            keldra_atomic_program::ProgramInput::default()
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
        authorize_logical(expanded.clone()).await?;
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
            let result = self.load_distributed_result(committed, true).await?;
            authorize_canonical(distributed_canonical_authorization(
                &expanded,
                &result.alias_targets,
            )?)
            .await?;
            return Ok(result);
        }

        let alias_bindings = distributed
            .resolve_alias_bindings(&expanded, nomination, budget)
            .await?;
        let canonical_paths = alias_bindings
            .iter()
            .map(|binding| {
                (
                    binding.requested_path.clone(),
                    binding.canonical_path.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        authorize_canonical(distributed_canonical_authorization(
            &expanded,
            &canonical_paths,
        )?)
        .await?;

        let prepared = distributed
            .prepare_distributed(
                &evaluator,
                &context,
                &invocation,
                durability_class,
                &canonical_paths,
                &alias_bindings,
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
        let begun = self
            .decisions
            .submit(Command::BeginBatch(BeginBatch {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                authority: super::decision_bundle_authority(prepared.prepared.authority),
                invocation_id: consensus_invocation_id,
                input_fingerprint: InvocationFingerprint(fingerprint),
                bundle_ref: BundleRef {
                    hash: prepared.prepared.bundle.hash,
                    length: prepared.prepared.bundle.length,
                },
                bundle_hash: BundleHash(prepared.prepared.hash.0),
                durability_class: DurabilityClass(
                    keldra_store::ProgramDurabilityClassHash::for_class(durability_class).0,
                ),
                durability_evidence_hash: DurabilityEvidenceHash(evidence_hash.0),
                participant_manifest_hash: ParticipantManifestHash(
                    prepared.prepared.participant_manifest_hash,
                ),
                proposal_at_unix_millis,
                replay_expires_at_unix_millis,
            }))
            .await
            .map_err(super::decision_status)?;
        let prepared_batch = match super::expect_batch_begun(begun.result)? {
            BeginResult::AlreadyCommitted(committed) => {
                drop(prepared.lease);
                return self
                    .load_distributed_result(committed.invocation, true)
                    .await;
            }
            BeginResult::Prepared { batch, .. } => batch,
        };
        let stages = match distributed
            .stage_prepared(
                &prepared.prepared,
                &prepared.record,
                prepared_batch.begin_cursor,
                nomination,
                budget,
            )
            .await
        {
            Ok(stages) => stages,
            Err(error) => {
                self.abort_prepared_batch(prepared_batch).await?;
                return Err(error);
            }
        };
        let reservations = prepared
            .record
            .reservations(
                prepared_batch.begin_cursor,
                consensus_invocation_id.0,
                prepared.prepared.hash,
                self.node.0,
                nomination.nomination_log_index,
                distributed
                    .objects
                    .program_mutation_context()?
                    .active_placement_log_id,
            )
            .map_err(super::program_store_status)?;
        if let Err(error) = distributed
            .reserve_participants(&reservations, nomination, budget)
            .await
        {
            self.abort_prepared_batch(prepared_batch).await?;
            distributed
                .release_participants(&reservations, None, nomination, budget)
                .await?;
            return Err(error);
        }
        let committed = self
            .decisions
            .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                begin_cursor: prepared_batch.begin_cursor,
                invocation_id: consensus_invocation_id,
                participant_manifest_hash: ParticipantManifestHash(
                    prepared.prepared.participant_manifest_hash,
                ),
            }))
            .await
            .map_err(super::decision_status)?;
        let committed = super::expect_batch_committed(committed.result)?;
        distributed
            .commit_participants(
                &reservations,
                committed.invocation.committed_batch.commit_cursor,
                nomination,
                budget,
            )
            .await?;
        let finalized = distributed
            .finalize(
                &stages,
                committed.invocation.committed_batch.commit_cursor,
                nomination,
                budget,
            )
            .await?;
        self.store
            .publish_atomic_batch(
                SealedAtomicBatchPublication::from_prepared(
                    committed.invocation.committed_batch.commit_cursor,
                    prepared.prepared.bundle,
                    prepared.prepared.hash,
                    &prepared.record,
                    &stages.paths,
                    &finalized.paths,
                    &finalized.alias_registries,
                )
                .map_err(super::program_store_status)?,
            )
            .await
            .map_err(super::program_store_status)?;
        self.advance_finalized_through(
            nomination,
            committed.invocation.committed_batch.commit_cursor,
        )
        .await?;
        distributed
            .release_participants(
                &reservations,
                Some(committed.invocation.committed_batch.commit_cursor),
                nomination,
                budget,
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
        let preparing = state.preparing_batch();
        drop(state);
        if let Some(prepared_batch) = preparing {
            let (prepared, record) = match distributed.recover_prepared(prepared_batch).await {
                Ok(value) => value,
                Err(error)
                    if matches!(
                        error.code(),
                        tonic::Code::DataLoss
                            | tonic::Code::InvalidArgument
                            | tonic::Code::FailedPrecondition
                    ) =>
                {
                    self.abort_prepared_batch(prepared_batch).await?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let stages = distributed
                .stage_prepared(
                    &prepared,
                    &record,
                    prepared_batch.begin_cursor,
                    nomination,
                    Duration::from_secs(30),
                )
                .await?;
            let reservations = record
                .reservations(
                    prepared_batch.begin_cursor,
                    prepared_batch.request.invocation_id.0,
                    prepared.hash,
                    nomination.executor.0,
                    nomination.nomination_log_index,
                    distributed
                        .objects
                        .program_mutation_context()?
                        .active_placement_log_id,
                )
                .map_err(super::program_store_status)?;
            if let Err(error) = distributed
                .reserve_participants(&reservations, nomination, Duration::from_secs(30))
                .await
            {
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument
                        | tonic::Code::FailedPrecondition
                        | tonic::Code::DataLoss
                ) {
                    self.abort_prepared_batch(prepared_batch).await?;
                    distributed
                        .release_participants(
                            &reservations,
                            None,
                            nomination,
                            Duration::from_secs(30),
                        )
                        .await?;
                }
                return Err(error);
            }
            let committed = self
                .decisions
                .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
                    executor: nomination.executor,
                    nomination_log_index: nomination.nomination_log_index,
                    begin_cursor: prepared_batch.begin_cursor,
                    invocation_id: prepared_batch.request.invocation_id,
                    participant_manifest_hash: prepared_batch.request.participant_manifest_hash,
                }))
                .await
                .map_err(super::decision_status)?;
            let committed = super::expect_batch_committed(committed.result)?;
            distributed
                .commit_participants(
                    &reservations,
                    committed.invocation.committed_batch.commit_cursor,
                    nomination,
                    Duration::from_secs(30),
                )
                .await?;
            let _ = stages;
        }
        let state = self.decisions.state().map_err(super::decision_status)?;
        let invocations = state.unfinalized_invocations().collect::<Vec<_>>();
        drop(state);
        let mut through = None;
        let mut completed_reservations = Vec::new();
        for invocation in invocations {
            let (record, stages) = distributed.recover_record(invocation).await?;
            let reservations = record
                .reservations(
                    invocation.committed_batch.begin_cursor,
                    invocation.invocation_id.0,
                    PreparedBundleHash(invocation.committed_batch.bundle_hash.0),
                    nomination.executor.0,
                    nomination.nomination_log_index,
                    distributed
                        .objects
                        .program_mutation_context()?
                        .active_placement_log_id,
                )
                .map_err(super::program_store_status)?;
            distributed
                .reserve_participants(&reservations, nomination, Duration::from_secs(30))
                .await?;
            distributed
                .commit_participants(
                    &reservations,
                    invocation.committed_batch.commit_cursor,
                    nomination,
                    Duration::from_secs(30),
                )
                .await?;
            let finalized = distributed
                .finalize(
                    &stages,
                    invocation.committed_batch.commit_cursor,
                    nomination,
                    Duration::from_secs(30),
                )
                .await?;
            self.store
                .publish_atomic_batch(
                    SealedAtomicBatchPublication::from_prepared(
                        invocation.committed_batch.commit_cursor,
                        PreparedBundleRef {
                            hash: invocation.committed_batch.bundle_ref.hash,
                            length: invocation.committed_batch.bundle_ref.length,
                        },
                        PreparedBundleHash(invocation.committed_batch.bundle_hash.0),
                        &record,
                        &stages.paths,
                        &finalized.paths,
                        &finalized.alias_registries,
                    )
                    .map_err(super::program_store_status)?,
                )
                .await
                .map_err(super::program_store_status)?;
            through = Some(invocation.committed_batch.commit_cursor);
            completed_reservations.push((reservations, invocation.committed_batch.commit_cursor));
        }
        if let Some(through) = through {
            self.advance_finalized_through(nomination, through).await?;
        }
        for (reservations, commit_cursor) in completed_reservations {
            distributed
                .release_participants(
                    &reservations,
                    Some(commit_cursor),
                    nomination,
                    Duration::from_secs(30),
                )
                .await?;
        }
        Ok(())
    }

    pub(super) async fn load_distributed_result(
        &self,
        invocation: CommittedInvocation,
        replayed: bool,
    ) -> Result<super::InvokedProgramResult, Status> {
        let (record, _) = self.distributed()?.recover_record(invocation).await?;
        Ok(result_from_record(&record, invocation, replayed))
    }

    pub(super) fn distributed(&self) -> Result<&DistributedPrograms, Status> {
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
}

pub(super) struct DistributedProgramStages {
    pub(super) paths: Vec<ProgramPathStage>,
    pub(super) alias_registries: Vec<ProgramAliasRegistryStage>,
}

pub(super) struct DistributedProgramFinalizations {
    pub(super) paths: Vec<ProgramPathMutation>,
    pub(super) alias_registries: Vec<ProgramAliasRegistryMutation>,
}

impl DistributedPrograms {
    pub(super) fn mutation_context(&self) -> Result<keldra_store::ObjectMutationContext, Status> {
        self.objects.program_mutation_context()
    }

    pub(super) async fn prepare_builtin(
        &self,
        plan: &BuiltInObjectTransactionPlan,
        durability_class: &str,
    ) -> Result<(PreparedProgramBundle, PreparedProgramRecord), Status> {
        let mut prepared = self
            .store
            .prepare_builtin_object_transaction(plan)
            .await
            .map_err(super::program_store_status)?;
        let record = self
            .store
            .prepared_program_record(&prepared)
            .await
            .map_err(super::program_store_status)?;
        for write in record.writes() {
            if let Some(reference) = write.version().blob.as_ref() {
                let staged_source = plan.writes.iter().find_map(|planned| {
                    (&planned.path == write.path()).then_some(&planned.payload)
                });
                match staged_source {
                    Some(keldra_store::BuiltInWritePayload::StagedReference {
                        upload_source_node_id,
                        ..
                    }) => {
                        self.objects
                            .prepare_program_blob_from_source(
                                reference,
                                NodeId(*upload_source_node_id),
                            )
                            .await?;
                    }
                    // The exact retained source reservation keeps the already
                    // published same-bucket payload alive until the
                    // destination reference increment commits.
                    Some(keldra_store::BuiltInWritePayload::ExistingReference(_)) => {}
                    Some(
                        keldra_store::BuiltInWritePayload::Inline { .. }
                        | keldra_store::BuiltInWritePayload::Tombstone,
                    )
                    | None => self.objects.prepare_program_blob(reference).await?,
                }
            }
        }
        self.objects
            .prepare_program_blob(&BlobRef::from(prepared.bundle))
            .await?;
        prepared
            .attest_remote_durability(durability_class)
            .map_err(super::program_store_status)?;
        Ok((prepared, record))
    }

    pub(super) async fn recover_prepared(
        &self,
        prepared_batch: PreparedBatch,
    ) -> Result<(PreparedProgramBundle, PreparedProgramRecord), Status> {
        let batch = prepared_batch.request;
        let decision_program_hash = match batch.authority {
            AtomicBundleAuthority::StoredProgram { program_hash, .. } => program_hash,
            AtomicBundleAuthority::BuiltInObjectTransaction { .. } => {
                keldra_consensus::ProgramHash([0; 32])
            }
            AtomicBundleAuthority::LegacyProgramOnly { .. } => {
                return Err(Status::data_loss(
                    "legacy authority cannot own a preparing transaction",
                ));
            }
        };
        let bundle = PreparedBundleRef {
            hash: batch.bundle_ref.hash,
            length: batch.bundle_ref.length,
        };
        let bytes = self.reader.read_blob_bytes(&BlobRef::from(bundle)).await?;
        let record = PreparedProgramRecord::decode_distributed(
            &bytes,
            bundle,
            PreparedBundleHash(batch.bundle_hash.0),
            ProgramHash(decision_program_hash.0),
        )
        .map_err(super::program_store_status)?;
        if record.authority() != super::store_bundle_authority(batch.authority)
            || record
                .participant_manifest_hash(PreparedBundleHash(batch.bundle_hash.0))
                .map_err(super::program_store_status)?
                != batch.participant_manifest_hash.0
        {
            return Err(Status::data_loss(
                "prepared transaction authority differs from Raft BeginBatch",
            ));
        }
        Ok((
            PreparedProgramBundle {
                hash: PreparedBundleHash(batch.bundle_hash.0),
                source_bundle_hash: record.source_bundle_hash(),
                program_hash: ProgramHash(decision_program_hash.0),
                authority: record.authority(),
                participant_manifest_hash: batch.participant_manifest_hash.0,
                bundle,
                durability_evidence_hash: keldra_store::ProgramDurabilityEvidenceHash(
                    batch.durability_evidence_hash.0,
                ),
                durability: keldra_store::ProgramDurabilityEvidence {
                    format: 1,
                    bundle,
                    scope: keldra_store::ProgramDurabilityScope::ConfiguredRemote {
                        class: "recovery".into(),
                    },
                    provider_receipt: Vec::new(),
                },
            },
            record,
        ))
    }

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
        key: &keldra_store::ObjectKey,
        expected_hash: ProgramHash,
    ) -> Result<keldra_store::VerifiedProgramDefinition, Status> {
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
        keldra_store::VerifiedProgramDefinition::from_bytes(&bytes, expected_hash)
            .map_err(super::program_store_status)
    }

    pub(super) fn evaluator(
        &self,
        definition: &keldra_store::VerifiedProgramDefinition,
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

    pub(super) async fn resolve_alias_bindings(
        &self,
        paths: &[keldra_atomic_program::ExpandedProgramPath],
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<Vec<ProgramAliasBinding>, Status> {
        let mut bindings = Vec::with_capacity(paths.len());
        for expanded in paths {
            let requested_key = keldra_store::ObjectKey::new(
                &expanded.path.tenant,
                &expanded.path.bucket,
                &expanded.path.path,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let requested = self.reader.open(&requested_key, None).await?;
            let mut alias_binding = None;
            let requested_version = requested.as_ref().map(|object| object.version.clone());
            if let Some(mut object) = requested
                && object.version.protected_link_descriptor
                && object.version.content_type.as_deref()
                    == Some(keldra_store::OBJECT_LINK_CONTENT_TYPE)
            {
                if let Some(mut payload) = object.payload.take() {
                    let mut descriptor_bytes = Vec::new();
                    payload
                        .read_to_end(&mut descriptor_bytes)
                        .map_err(|error| {
                            Status::internal(format!("read alias descriptor: {error}"))
                        })?;
                    if let Ok(descriptor) =
                        keldra_store::ObjectLinkDescriptor::decode(&descriptor_bytes)
                        && let Ok(canonical_path) = ObjectPath::new(
                            &expanded.path.tenant,
                            &expanded.path.bucket,
                            descriptor.target_path(),
                        )
                    {
                        let (tenant_id, bucket_id) = self
                            .names
                            .resolve_bucket_ids(&canonical_path.tenant, &canonical_path.bucket)
                            .await?;
                        let registry = self
                            .read_alias_registry(
                                tenant_id,
                                bucket_id,
                                &canonical_path.path,
                                nomination,
                                budget,
                            )
                            .await?;
                        if registry.as_ref().is_some_and(|registry| {
                            registry.aliases.binary_search(&expanded.path.path).is_ok()
                        }) {
                            let canonical_key = keldra_store::ObjectKey::new(
                                &canonical_path.tenant,
                                &canonical_path.bucket,
                                &canonical_path.path,
                            )
                            .map_err(|error| Status::data_loss(error.to_string()))?;
                            let canonical = self
                                .reader
                                .open(&canonical_key, None)
                                .await?
                                .ok_or_else(|| {
                                    Status::data_loss("proven alias target does not exist")
                                })?;
                            if canonical.version.deleted {
                                return Err(Status::data_loss("proven alias target is deleted"));
                            }
                            if canonical.version.protected_link_descriptor {
                                return Err(Status::data_loss(
                                    "proven alias target is itself a protected alias descriptor",
                                ));
                            }
                            alias_binding = Some(ProgramAliasBinding {
                                requested_path: expanded.path.clone(),
                                canonical_path,
                                descriptor_version: Some(object.version),
                                descriptor_bytes: Some(descriptor_bytes),
                                canonical_version: Some(canonical.version),
                                alias_registry: registry,
                            });
                        }
                    }
                }
            }
            if alias_binding.is_none()
                && requested_version
                    .as_ref()
                    .is_some_and(|version| version.protected_link_descriptor)
            {
                return Err(Status::data_loss(
                    "protected alias descriptor has no exact target-sidecar provenance",
                ));
            }
            let binding = if let Some(binding) = alias_binding {
                binding
            } else {
                let (tenant_id, bucket_id) = self
                    .names
                    .resolve_bucket_ids(&expanded.path.tenant, &expanded.path.bucket)
                    .await?;
                ProgramAliasBinding {
                    requested_path: expanded.path.clone(),
                    canonical_path: expanded.path.clone(),
                    descriptor_version: None,
                    descriptor_bytes: None,
                    canonical_version: requested_version,
                    alias_registry: self
                        .read_alias_registry(
                            tenant_id,
                            bucket_id,
                            &expanded.path.path,
                            nomination,
                            budget,
                        )
                        .await?,
                }
            };
            binding.validate().map_err(super::program_store_status)?;
            bindings.push(binding);
        }
        bindings.sort_by(|left, right| left.requested_path.cmp(&right.requested_path));
        let mut canonical = std::collections::BTreeSet::new();
        if bindings
            .iter()
            .any(|binding| !canonical.insert(binding.canonical_path.clone()))
        {
            return Err(Status::invalid_argument(
                "one physical object is bound more than once after alias resolution",
            ));
        }
        Ok(bindings)
    }

    async fn read_alias_registry(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        canonical_path: &str,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<Option<keldra_store::ObjectAliasRegistry>, Status> {
        let placement = self.objects.current_program_placement()?;
        let group = self
            .objects
            .program_replica_group(tenant_id, bucket_id, canonical_path)?;
        let coordinator = group.coordinator();
        let registry = if coordinator == self.local_node {
            self.store
                .object_alias_registry(tenant_id, bucket_id, canonical_path)
                .map_err(super::mutation_status)?
        } else {
            let address = placement.address(coordinator).ok_or_else(|| {
                Status::unavailable(format!(
                    "ACTIVE alias target authority {} has no peer address",
                    coordinator.0
                ))
            })?;
            self.peers
                .read_program_alias_registry(
                    coordinator,
                    &address.0,
                    nomination.nomination_log_index,
                    tenant_id,
                    bucket_id,
                    canonical_path,
                    budget,
                )
                .await?
        };
        if self.objects.current_program_placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "placement changed during alias-registry read",
            ));
        }
        Ok(registry)
    }

    pub(super) async fn prepare_distributed(
        &self,
        evaluator: &DistributedEvaluator,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
        durability_class: &str,
        canonical_paths: &BTreeMap<ObjectPath, ObjectPath>,
        alias_bindings: &[ProgramAliasBinding],
    ) -> Result<PreparedDistributedInvocation, Status> {
        let lease = evaluator
            .engine
            .prepare_canonicalized(context, invocation, canonical_paths)
            .await
            .map_err(super::engine_status)?;
        let previous = evaluator.previous_versions()?;
        let mut prepared = self
            .store
            .prepare_distributed_program_bundle_with_aliases(
                evaluator.program_hash,
                lease.bundle(),
                &previous,
                alias_bindings,
            )
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

        Ok(PreparedDistributedInvocation {
            lease,
            prepared,
            record,
        })
    }

    pub(super) async fn stage_prepared(
        &self,
        prepared: &PreparedProgramBundle,
        record: &PreparedProgramRecord,
        begin_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<DistributedProgramStages, Status> {
        let mut stages = Vec::with_capacity(record.writes().len());
        for write in record.writes() {
            let (tenant_id, bucket_id) = self
                .names
                .resolve_bucket_ids(&write.path().tenant, &write.path().bucket)
                .await?;
            stages.push(
                path_stage_from_prepared(prepared, write, begin_cursor, tenant_id, bucket_id)
                    .map_err(super::program_store_status)?,
            );
        }
        for stage in &stages {
            self.stage_path(stage, nomination, budget).await?;
        }
        Ok(DistributedProgramStages {
            paths: stages,
            alias_registries: alias_registry_stages_from_prepared(prepared, record, begin_cursor)
                .map_err(super::program_store_status)?,
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

    pub(super) async fn reserve_participants(
        &self,
        reservations: &[ProgramReservation],
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        for reservation in reservations {
            self.apply_reservation_operation(reservation, None, None, nomination, budget)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn commit_participants(
        &self,
        reservations: &[ProgramReservation],
        commit_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        for reservation in reservations {
            self.apply_reservation_operation(
                reservation,
                Some(Some(commit_cursor)),
                None,
                nomination,
                budget,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn release_participants(
        &self,
        reservations: &[ProgramReservation],
        finalized_commit_cursor: Option<u64>,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        for reservation in reservations {
            self.apply_reservation_operation(
                reservation,
                Some(None),
                finalized_commit_cursor,
                nomination,
                budget,
            )
            .await?;
        }
        Ok(())
    }

    async fn apply_reservation_operation(
        &self,
        reservation: &ProgramReservation,
        commit_or_release: Option<Option<u64>>,
        finalized_commit_cursor: Option<u64>,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<(), Status> {
        let placement = self.objects.current_program_placement()?;
        let (tenant_id, bucket_id) = reservation.stable_bucket_ids();
        let path = reservation.path();
        let group = self
            .objects
            .program_replica_group(tenant_id, bucket_id, &path.path)?;
        let mut acknowledged = Vec::new();
        let mut first_terminal_error = None;
        let mut terminal_rejections = 0_usize;
        for node in group.replicas().iter().copied() {
            let result = if node == self.local_node {
                match commit_or_release {
                    None => self.store.reserve_program_participant(reservation).await,
                    Some(Some(cursor)) => self
                        .store
                        .commit_program_participant(reservation, cursor)
                        .await
                        .map(|_| ()),
                    Some(None) => {
                        self.store
                            .release_program_participant(reservation, finalized_commit_cursor)
                            .await
                    }
                }
                .map_err(super::mutation_status)
            } else {
                let address = placement.address(node).ok_or_else(|| {
                    Status::unavailable(format!(
                        "ACTIVE reservation replica {} has no peer address",
                        node.0
                    ))
                })?;
                match commit_or_release {
                    None => {
                        self.peers
                            .reserve_program_participant(
                                node,
                                &address.0,
                                nomination.nomination_log_index,
                                reservation,
                                budget,
                            )
                            .await
                    }
                    Some(Some(cursor)) => {
                        self.peers
                            .commit_program_participant(
                                node,
                                &address.0,
                                nomination.nomination_log_index,
                                cursor,
                                reservation,
                                budget,
                            )
                            .await
                    }
                    Some(None) => {
                        self.peers
                            .release_program_participant(
                                node,
                                &address.0,
                                nomination.nomination_log_index,
                                finalized_commit_cursor,
                                reservation,
                                budget,
                            )
                            .await
                    }
                }
            };
            match result {
                Ok(()) => acknowledged.push(node),
                Err(error) => {
                    tracing::warn!(node_id = node.0, %error, "atomic reservation replica operation failed");
                    if matches!(
                        error.code(),
                        tonic::Code::InvalidArgument
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::DataLoss
                    ) && first_terminal_error.is_none()
                    {
                        terminal_rejections += 1;
                        first_terminal_error = Some(error);
                    } else if matches!(
                        error.code(),
                        tonic::Code::InvalidArgument
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::DataLoss
                    ) {
                        terminal_rejections += 1;
                    }
                }
            }
        }
        // Reserve/commit use the same exact-path quorum as object reads and
        // replica mutation. Quorum intersection plus the per-path lock means
        // every later authoritative mutation observes at least one reserved
        // replica and repair cannot select an unreserved minority. Cleanup is
        // sent to all replicas so a missed commit cannot leave local poison.
        let required = if matches!(commit_or_release, Some(None)) {
            group.replicas().len()
        } else {
            group.required_acknowledgements()
        };
        if acknowledged.len() < required {
            if terminal_rejections_make_threshold_impossible(
                group.replicas().len(),
                terminal_rejections,
                required,
            ) && let Some(error) = first_terminal_error
            {
                return Err(error);
            }
            return Err(Status::unavailable(format!(
                "atomic reservation operation reached {} of {} replicas",
                acknowledged.len(),
                group.replicas().len()
            )));
        }
        if self.objects.current_program_placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "placement changed during atomic reservation operation",
            ));
        }
        Ok(())
    }

    pub(super) async fn finalize(
        &self,
        stages: &DistributedProgramStages,
        commit_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<DistributedProgramFinalizations, Status> {
        let mut paths = Vec::with_capacity(stages.paths.len());
        for stage in &stages.paths {
            paths.push(
                self.finalize_path(stage, commit_cursor, nomination, budget)
                    .await?,
            );
        }
        let mut alias_registries = Vec::with_capacity(stages.alias_registries.len());
        for stage in &stages.alias_registries {
            alias_registries.push(
                self.finalize_alias_registry(stage, commit_cursor, nomination, budget)
                    .await?,
            );
        }
        Ok(DistributedProgramFinalizations {
            paths,
            alias_registries,
        })
    }

    async fn finalize_path(
        &self,
        stage: &ProgramPathStage,
        commit_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<ProgramPathMutation, Status> {
        let placement = self.objects.current_program_placement()?;
        let mutation_context = atomic_mutation_context(placement.fence(), nomination);
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
                    mutation_context,
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
        Ok(mutation)
    }

    async fn finalize_alias_registry(
        &self,
        stage: &ProgramAliasRegistryStage,
        commit_cursor: u64,
        nomination: ExecutorNomination,
        budget: Duration,
    ) -> Result<ProgramAliasRegistryMutation, Status> {
        let placement = self.objects.current_program_placement()?;
        let mutation_context = atomic_mutation_context(placement.fence(), nomination);
        let group = self.objects.program_replica_group(
            stage.tenant_id,
            stage.bucket_id,
            &stage.target.path,
        )?;
        let coordinator = group.coordinator();
        let mutation = if coordinator == self.local_node {
            self.store
                .coordinate_program_alias_registry_finalization(
                    stage.clone(),
                    commit_cursor,
                    mutation_context,
                )
                .await
                .map_err(super::program_store_status)?
        } else {
            let address = placement.address(coordinator).ok_or_else(|| {
                Status::unavailable(format!(
                    "ACTIVE alias target authority {} has no peer address",
                    coordinator.0
                ))
            })?;
            self.peers
                .coordinate_program_alias_registry_finalization(
                    coordinator,
                    &address.0,
                    nomination.nomination_log_index,
                    commit_cursor,
                    stage,
                    budget,
                )
                .await?
        };
        mutation.validate().map_err(super::program_store_status)?;
        if mutation.stage != *stage || mutation.commit_cursor != commit_cursor {
            return Err(Status::data_loss(
                "alias target authority returned another sidecar finalization",
            ));
        }
        let mut durable = vec![coordinator];
        for replica in group
            .replicas()
            .iter()
            .copied()
            .filter(|replica| *replica != coordinator)
        {
            let result = if replica == self.local_node {
                self.store
                    .apply_program_alias_registry_finalization_replica(&mutation, mutation_context)
                    .await
                    .map(|_| ())
                    .map_err(super::program_store_status)
            } else {
                let address = placement.address(replica).ok_or_else(|| {
                    Status::unavailable(format!(
                        "ACTIVE alias target replica {} has no peer address",
                        replica.0
                    ))
                })?;
                self.peers
                    .apply_program_alias_registry_finalization(
                        replica,
                        &address.0,
                        nomination.nomination_log_index,
                        &mutation,
                        budget,
                    )
                    .await
                    .map(|_| ())
            };
            match result {
                Ok(()) => durable.push(replica),
                Err(error) => tracing::warn!(
                    node_id = replica.0,
                    %error,
                    "alias target replica finalization failed"
                ),
            }
        }
        if !group.is_acknowledged_by(&durable) {
            return Err(Status::unavailable(format!(
                "alias target finalization reached {} of {} required replicas",
                durable.len(),
                group.required_acknowledgements()
            )));
        }
        if self.objects.current_program_placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "placement changed during alias target finalization",
            ));
        }
        Ok(mutation)
    }

    pub(super) async fn recover_record(
        &self,
        invocation: CommittedInvocation,
    ) -> Result<(PreparedProgramRecord, DistributedProgramStages), Status> {
        let batch = invocation.committed_batch;
        let decision_program_hash = match batch.authority {
            AtomicBundleAuthority::StoredProgram { program_hash, .. }
            | AtomicBundleAuthority::LegacyProgramOnly { program_hash, .. } => program_hash,
            AtomicBundleAuthority::BuiltInObjectTransaction { .. } => {
                keldra_consensus::ProgramHash([0; 32])
            }
        };
        let bundle = PreparedBundleRef {
            hash: batch.bundle_ref.hash,
            length: batch.bundle_ref.length,
        };
        let bytes = self.reader.read_blob_bytes(&BlobRef::from(bundle)).await?;
        let record = PreparedProgramRecord::decode_distributed(
            &bytes,
            bundle,
            PreparedBundleHash(batch.bundle_hash.0),
            ProgramHash(decision_program_hash.0),
        )
        .map_err(super::program_store_status)?;
        if record.authority() != super::store_bundle_authority(batch.authority)
            || record
                .participant_manifest_hash(PreparedBundleHash(batch.bundle_hash.0))
                .map_err(super::program_store_status)?
                != batch.participant_manifest_hash.0
        {
            return Err(Status::data_loss(
                "prepared bundle authority or participant manifest differs from Raft",
            ));
        }
        let prepared = PreparedProgramBundle {
            hash: PreparedBundleHash(batch.bundle_hash.0),
            source_bundle_hash: record.source_bundle_hash(),
            program_hash: ProgramHash(decision_program_hash.0),
            authority: super::store_bundle_authority(batch.authority),
            participant_manifest_hash: batch.participant_manifest_hash.0,
            bundle,
            durability_evidence_hash: keldra_store::ProgramDurabilityEvidenceHash(
                batch.durability_evidence_hash.0,
            ),
            durability: keldra_store::ProgramDurabilityEvidence {
                format: 1,
                bundle,
                scope: keldra_store::ProgramDurabilityScope::ConfiguredRemote {
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
                path_stage_from_prepared(
                    &prepared,
                    write,
                    batch.begin_cursor,
                    tenant_id,
                    bucket_id,
                )
                .map_err(super::program_store_status)?,
            );
        }
        let alias_registries =
            alias_registry_stages_from_prepared(&prepared, &record, batch.begin_cursor)
                .map_err(super::program_store_status)?;
        Ok((
            record,
            DistributedProgramStages {
                paths: stages,
                alias_registries,
            },
        ))
    }
}

fn terminal_rejections_make_threshold_impossible(
    replicas: usize,
    terminal_rejections: usize,
    required: usize,
) -> bool {
    replicas.saturating_sub(terminal_rejections) < required
}

fn distributed_canonical_authorization(
    expanded: &[keldra_atomic_program::ExpandedProgramPath],
    canonical_paths: &BTreeMap<ObjectPath, ObjectPath>,
) -> Result<Vec<keldra_atomic_program::ExpandedProgramPath>, Status> {
    expanded
        .iter()
        .filter_map(|logical| {
            let canonical = match canonical_paths.get(&logical.path) {
                Some(canonical) => canonical,
                None => {
                    return Some(Err(Status::data_loss(
                        "stored-program canonical path binding is absent",
                    )));
                }
            };
            (canonical != &logical.path).then(|| {
                Ok(keldra_atomic_program::ExpandedProgramPath {
                    path: canonical.clone(),
                    intent: logical.intent,
                })
            })
        })
        .collect()
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
    ) -> Result<Vec<keldra_atomic_program::ExpandedProgramPath>, Status> {
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
            let key = keldra_store::ObjectKey::new(&path.tenant, &path.bucket, &path.path)
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

fn atomic_mutation_context(
    placement: PlacementLogId,
    nomination: ExecutorNomination,
) -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: placement,
        // The executor nomination is the atomic mutation fence. The ordinary
        // serving-lease term belongs to a separate mutation path.
        serving_fence_term: nomination.nomination_log_index,
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

pub(super) fn result_from_record(
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
                keldra_store::PublishedProgramVersion {
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
        program_hash: match invocation.committed_batch.authority {
            AtomicBundleAuthority::StoredProgram { program_hash, .. }
            | AtomicBundleAuthority::LegacyProgramOnly { program_hash, .. } => program_hash.0,
            AtomicBundleAuthority::BuiltInObjectTransaction { .. } => [0; 32],
        },
        published_versions,
        asserted_versions: record.asserted_versions(),
        alias_targets: record.alias_targets(),
        replayed,
        replay_guarantee_expires_at_unix_millis: invocation.replay_expires_at_unix_millis,
    }
}

#[cfg(test)]
mod reservation_tests {
    use keldra_consensus::{ExecutorNomination, NodeId};
    use keldra_store::PlacementLogId;

    use super::{atomic_mutation_context, terminal_rejections_make_threshold_impossible};

    #[test]
    fn local_atomic_context_uses_the_executor_nomination_fence() {
        let ordinary_serving_term = 7;
        let nomination = ExecutorNomination {
            executor: NodeId(2),
            nomination_log_index: 29,
        };
        assert_ne!(nomination.nomination_log_index, ordinary_serving_term);

        let placement = PlacementLogId { term: 5, index: 22 };
        let context = atomic_mutation_context(placement, nomination);

        assert_eq!(context.active_placement_log_id, placement);
        assert_eq!(context.serving_fence_term, nomination.nomination_log_index);
        assert_ne!(context.serving_fence_term, ordinary_serving_term);
    }

    #[test]
    fn terminal_error_is_returned_only_when_success_threshold_is_impossible() {
        assert!(!terminal_rejections_make_threshold_impossible(3, 1, 2));
        assert!(terminal_rejections_make_threshold_impossible(3, 2, 2));
        assert!(terminal_rejections_make_threshold_impossible(3, 1, 3));
    }
}
