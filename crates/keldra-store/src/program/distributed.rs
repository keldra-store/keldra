use keldra_atomic_program::{ObjectPath, ObservedHead};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

use super::*;
use crate::key::{BucketId, BucketIdentity, TenantId};
use crate::store::{
    CF_HEADS, CF_METADATA, CF_VERSIONS, PendingLocalChange, StoredVersion, StoredVersionRetention,
    VERSION_HIGH_WATERMARK_KEY, version_key,
};
use crate::{
    MUTATION_STAMP_FORMAT, MutationStamp, ObjectMutationContext, ObjectVersioning, ReferenceProof,
    ReferenceProofMutation, SourceId,
};

pub const PROGRAM_PATH_STAGE_FORMAT: u16 = 1;
pub const PROGRAM_PATH_MUTATION_FORMAT: u16 = 1;
pub const PROGRAM_ALIAS_REGISTRY_STAGE_FORMAT: u16 = 1;
pub const PROGRAM_ALIAS_REGISTRY_MUTATION_FORMAT: u16 = 1;

/// One immutable, path-scoped slice of the ordinary prepared bundle.
///
/// Its canonical bytes are sealed through the normal blob plane on every
/// required metadata replica before Raft commits. There is intentionally no
/// stage column family, lookup index, receipt, or cleanup protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramPathStage {
    pub format: u16,
    pub begin_cursor: u64,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub authority: ProgramBundleAuthority,
    pub participant_manifest_hash: [u8; 32],
    pub tenant_id: u64,
    pub bucket_id: u64,
    /// Authoritative bucket governance sealed by the complete logical-record
    /// group in the participant manifest. Exact-path replicas are deliberately
    /// independent of replica-local bucket records.
    pub governance: ProgramGovernanceParticipant,
    pub path: ObjectPath,
    pub expected: ObservedHead,
    pub previous_version: Option<Version>,
    pub version: Version,
}

impl ProgramPathStage {
    pub fn encoded(&self) -> Result<Vec<u8>, ProgramStoreError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(program_storage_error)
    }

    pub fn blob_ref(&self) -> Result<BlobRef, ProgramStoreError> {
        let encoded = self.encoded()?;
        Ok(BlobRef {
            hash: *blake3::hash(&encoded).as_bytes(),
            length: encoded.len() as u64,
        })
    }

    pub fn validate(&self) -> Result<(), ProgramStoreError> {
        let valid_program_hash = match self.authority {
            ProgramBundleAuthority::StoredProgram { .. } => self.program_hash.0 != [0; 32],
            ProgramBundleAuthority::BuiltInObjectTransaction { .. } => {
                self.program_hash.0 == [0; 32]
            }
        };
        if self.format != PROGRAM_PATH_STAGE_FORMAT
            || self.begin_cursor == 0
            || self.bundle_hash.0 == [0; 32]
            || !valid_program_hash
            || self.participant_manifest_hash == [0; 32]
            || self.tenant_id == 0
            || self.bucket_id == 0
            || self.governance.tenant != self.path.tenant
            || self.governance.bucket != self.path.bucket
            || self.governance.tenant_id != self.tenant_id
            || self.governance.bucket_id != self.bucket_id
            || self.version.id.0 == 0
            || self.version.deleted != self.version.blob.is_none()
            || self.version.deleted && self.version.content_type.is_some()
            || self
                .version
                .content_type
                .as_ref()
                .is_some_and(|value| value.len() > crate::MAX_CONTENT_TYPE_BYTES)
        {
            return Err(ProgramStoreError::InvalidBundle(
                "distributed program path stage is malformed".into(),
            ));
        }
        self.authority
            .validate()
            .map_err(|message| ProgramStoreError::InvalidBundle(message.into()))?;
        self.governance
            .policy
            .validate()
            .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))?;
        ObjectKey::new(&self.path.tenant, &self.path.bucket, &self.path.path)
            .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))?;
        match (&self.expected, &self.previous_version) {
            (ObservedHead::NeverExisted, None) => {}
            (ObservedHead::Version { version }, Some(previous))
                if version.parse::<u64>().ok() == Some(previous.id.0)
                    && previous.id < self.version.id => {}
            _ => {
                return Err(ProgramStoreError::InvalidBundle(
                    "distributed stage predecessor does not match its observed head".into(),
                ));
            }
        }
        Ok(())
    }
}

/// The exact coordinator-produced result copied to the other complete-record
/// replicas after the Raft decision. It contains no payload bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramPathMutation {
    pub format: u16,
    pub commit_cursor: u64,
    pub stage: ProgramPathStage,
    pub stamp: MutationStamp,
    pub reference_deltas: Vec<ReferenceDelta>,
    pub accounting_transition: AccountingHeadTransition,
}

impl ProgramPathMutation {
    fn set_fingerprint(&mut self) -> Result<(), ProgramStoreError> {
        self.stamp.mutation_fingerprint = self.computed_fingerprint()?;
        Ok(())
    }

    pub fn computed_fingerprint(&self) -> Result<[u8; 32], ProgramStoreError> {
        let mut canonical = self.clone();
        canonical.stamp.mutation_fingerprint = [0; 32];
        let encoded = serde_json::to_vec(&canonical).map_err(program_storage_error)?;
        Ok(tagged_hash(b"keldra.program-path-mutation.v1", &encoded))
    }

    pub fn validate(&self) -> Result<(), ProgramStoreError> {
        self.stage.validate()?;
        self.accounting_transition
            .validate()
            .map_err(|error| ProgramStoreError::InvalidBundle(error.into()))?;
        let previous_live_length = self
            .stage
            .previous_version
            .as_ref()
            .and_then(live_version_length);
        let retention = stage_retention(&self.stage);
        let permitted_reference_deltas = permitted_reference_delta_variants(&self.stage)?;
        let expected_accounting_transition = stage_accounting_transition(&self.stage, retention);
        if self.format != PROGRAM_PATH_MUTATION_FORMAT
            || self.commit_cursor == 0
            || self.stamp.format != MUTATION_STAMP_FORMAT
            || self.stamp.active_placement_log_id.term == 0
            || self.stamp.active_placement_log_id.index == 0
            || self.stamp.serving_fence_term == 0
            || self.stamp.source_id.node_id == 0
            || self.stamp.source_id.source_epoch == [0; 32]
            || self.stamp.source_journal_position == 0
            || self.stamp.predecessor_version
                != self
                    .stage
                    .previous_version
                    .as_ref()
                    .map(|version| version.id)
            || self.stamp.program_commit_cursor != Some(self.commit_cursor)
            || self.reference_deltas.len() > crate::MAX_OBJECT_MUTATION_REFERENCE_DELTAS
            || self
                .reference_deltas
                .iter()
                .any(|delta| !matches!(delta.change, -1 | 1))
            || !permitted_reference_deltas.contains(&self.reference_deltas)
            || self.accounting_transition.previous_live_length != previous_live_length
            || self.accounting_transition.current_live_length
                != live_version_length(&self.stage.version)
            || (self.accounting_transition.logical_bytes_removed != 0
                && self.accounting_transition.logical_bytes_removed
                    != previous_live_length.unwrap_or(0))
            || self.accounting_transition != expected_accounting_transition
            || self.stamp.mutation_fingerprint != self.computed_fingerprint()?
        {
            return Err(ProgramStoreError::InvalidBundle(
                "distributed program path mutation is malformed".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedProgramPathFinalization {
    pub mutation: ProgramPathMutation,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaProgramPathApplied {
    pub version: VersionId,
    pub replayed: bool,
}

/// Sealed target-coordinator sidecar transition selected by a built-in plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramAliasRegistryStage {
    pub format: u16,
    pub begin_cursor: u64,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub authority: ProgramBundleAuthority,
    pub participant_manifest_hash: [u8; 32],
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub target: ObjectPath,
    pub expected: Option<crate::ObjectAliasRegistry>,
    pub replacement_aliases: Vec<String>,
}

impl ProgramAliasRegistryStage {
    pub fn validate(&self) -> Result<(), ProgramStoreError> {
        if self.format != PROGRAM_ALIAS_REGISTRY_STAGE_FORMAT
            || self.begin_cursor == 0
            || self.bundle_hash.0 == [0; 32]
            || self.participant_manifest_hash == [0; 32]
            || self.tenant_id == 0
            || self.bucket_id == 0
            || self
                .expected
                .as_ref()
                .is_some_and(|value| value.validate(&self.target.path).is_err())
            || !valid_alias_replacement(&self.target.path, &self.replacement_aliases)
            || self
                .expected
                .as_ref()
                .is_some_and(|value| value.aliases == self.replacement_aliases)
        {
            return Err(ProgramStoreError::InvalidBundle(
                "program alias-registry stage is malformed".into(),
            ));
        }
        self.authority
            .validate()
            .map_err(|message| ProgramStoreError::InvalidBundle(message.into()))?;
        ObjectKey::new(&self.target.tenant, &self.target.bucket, &self.target.path)
            .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramAliasRegistryMutation {
    pub format: u16,
    pub commit_cursor: u64,
    pub stage: ProgramAliasRegistryStage,
    pub replacement: Option<crate::ObjectAliasRegistry>,
}

impl ProgramAliasRegistryMutation {
    pub fn validate(&self) -> Result<(), ProgramStoreError> {
        self.stage.validate()?;
        let valid_replacement = match &self.replacement {
            None => self.stage.replacement_aliases.is_empty(),
            Some(replacement) => {
                let expected_revision = self
                    .stage
                    .expected
                    .as_ref()
                    .map_or(Some(1), |expected| expected.revision.checked_add(1));
                replacement.validate(&self.stage.target.path).is_ok()
                    && replacement.aliases == self.stage.replacement_aliases
                    && replacement.program_commit_cursor == Some(self.commit_cursor)
                    && Some(replacement.revision) == expected_revision
            }
        };
        if self.format != PROGRAM_ALIAS_REGISTRY_MUTATION_FORMAT
            || self.commit_cursor == 0
            || !valid_replacement
        {
            return Err(ProgramStoreError::InvalidBundle(
                "program alias-registry mutation is malformed".into(),
            ));
        }
        Ok(())
    }
}

impl StoredPreparedBundle {
    pub fn decode_distributed(
        bytes: &[u8],
        bundle: PreparedBundleRef,
        program_hash: ProgramHash,
    ) -> Result<Self, ProgramStoreError> {
        if bytes.len() as u64 != bundle.length || *blake3::hash(bytes).as_bytes() != bundle.hash.0 {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }
        let record: Self = serde_json::from_slice(bytes).map_err(program_storage_error)?;
        validate_prepared_record(&record)?;
        if record.program_hash != program_hash {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }
        Ok(record)
    }

    pub fn receipt(&self) -> &CommandReceipt {
        &self.receipt
    }

    pub fn writes(&self) -> &[PreparedVersionWrite] {
        &self.writes
    }

    pub fn builtin_plan(&self) -> Option<&BuiltInObjectTransactionPlan> {
        self.builtin_plan.as_ref()
    }

    pub fn alias_bindings(&self) -> &[ProgramAliasBinding] {
        &self.alias_bindings
    }

    pub(super) fn alias_registry_writes(
        &self,
    ) -> Result<
        Vec<(
            &ProgramObjectParticipant,
            Option<&crate::ObjectAliasRegistry>,
            &[String],
        )>,
        ProgramStoreError,
    > {
        if let Some(plan) = self.builtin_plan() {
            return plan
                .alias_registries
                .iter()
                .filter_map(|access| match access {
                    BuiltInAliasRegistryAccess::Read { .. } => None,
                    BuiltInAliasRegistryAccess::Write {
                        target_participant_index,
                        expected,
                        replacement_aliases,
                    } => Some(
                        plan.participant_manifest
                            .objects
                            .get(*target_participant_index as usize)
                            .map(|target| {
                                (target, expected.as_ref(), replacement_aliases.as_slice())
                            })
                            .ok_or_else(|| {
                                ProgramStoreError::InvalidBundle(
                                    "alias-registry target participant is absent".into(),
                                )
                            }),
                    ),
                })
                .collect();
        }
        self.alias_registry_transitions
            .iter()
            .map(|transition| {
                let target = self
                    .participant_manifest
                    .objects
                    .iter()
                    .find(|participant| participant.path == transition.target)
                    .ok_or_else(|| {
                        ProgramStoreError::InvalidBundle(
                            "stored alias transition target participant is absent".into(),
                        )
                    })?;
                Ok((
                    target,
                    Some(&transition.expected),
                    transition.replacement_aliases.as_slice(),
                ))
            })
            .collect()
    }

    pub fn asserted_versions(&self) -> BTreeMap<ObjectPath, Version> {
        let Some(plan) = self.builtin_plan.as_ref() else {
            return BTreeMap::new();
        };
        plan.assertions
            .iter()
            .filter_map(|assertion| match assertion {
                BuiltInTransactionAssertion::ClonePaths { .. } => None,
                BuiltInTransactionAssertion::PutImmutableMatches {
                    target_participant_index,
                    ..
                } => plan
                    .participant_manifest
                    .objects
                    .get(*target_participant_index as usize)
                    .and_then(|participant| {
                        participant
                            .condition
                            .head_version()
                            .cloned()
                            .map(|version| (participant.path.clone(), version))
                    }),
            })
            .collect()
    }

    pub fn alias_targets(&self) -> BTreeMap<ObjectPath, ObjectPath> {
        let mut targets = self
            .alias_bindings
            .iter()
            .map(|binding| {
                (
                    binding.requested_path.clone(),
                    binding.canonical_path.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let Some(plan) = self.builtin_plan.as_ref() else {
            return targets;
        };
        targets.extend(
            plan.alias_observations
                .iter()
                .filter_map(|observation| {
                    plan.participant_manifest
                        .objects
                        .get(observation.canonical_participant_index as usize)
                        .map(|canonical| {
                            (observation.requested_path.clone(), canonical.path.clone())
                        })
                })
                .collect::<BTreeMap<_, _>>(),
        );
        for assertion in &plan.assertions {
            if let BuiltInTransactionAssertion::ClonePaths {
                source_requested_path,
                destination_requested_path,
                source_participant_index,
                destination_participant_index,
            } = assertion
            {
                for (requested, participant_index) in [
                    (source_requested_path, source_participant_index),
                    (destination_requested_path, destination_participant_index),
                ] {
                    if let Some(canonical) = plan
                        .participant_manifest
                        .objects
                        .get(*participant_index as usize)
                    {
                        targets.insert(requested.clone(), canonical.path.clone());
                    }
                }
            }
        }
        targets
    }

    pub fn source_bundle_hash(&self) -> PreparedBundleHash {
        self.source_bundle_hash
    }

    pub fn authority(&self) -> ProgramBundleAuthority {
        self.authority
    }

    pub fn participant_manifest(&self) -> &ProgramParticipantManifest {
        &self.participant_manifest
    }

    pub fn participant_manifest_hash(&self) -> Result<[u8; 32], ProgramStoreError> {
        self.participant_manifest
            .hash()
            .map_err(ProgramStoreError::InvalidBundle)
    }
}

impl PreparedVersionWrite {
    pub fn path(&self) -> &ObjectPath {
        &self.path
    }

    pub fn expected(&self) -> &ObservedHead {
        &self.expected
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn previous_version(&self) -> Option<&Version> {
        self.previous_version.as_ref()
    }
}

impl PreparedProgramBundle {
    pub fn attest_remote_durability(&mut self, class: &str) -> Result<(), ProgramStoreError> {
        if class.trim().is_empty() {
            return Err(ProgramStoreError::DurabilityClassMismatch);
        }
        self.durability = ProgramDurabilityEvidence {
            format: DURABILITY_EVIDENCE_FORMAT,
            bundle: self.bundle,
            scope: ProgramDurabilityScope::ConfiguredRemote {
                class: class.to_owned(),
            },
            provider_receipt: Vec::new(),
        };
        self.durability_evidence_hash = self.durability.hash()?;
        Ok(())
    }
}

impl Store {
    pub async fn coordinate_program_alias_registry_finalization(
        &self,
        stage: ProgramAliasRegistryStage,
        commit_cursor: u64,
        context: ObjectMutationContext,
    ) -> Result<ProgramAliasRegistryMutation, ProgramStoreError> {
        stage.validate()?;
        let target = stage_key_from_path(&stage.target)?;
        let replacement = self
            .apply_alias_registry_transition(
                stage.tenant_id,
                stage.bucket_id,
                &target,
                stage.expected.as_ref(),
                &stage.replacement_aliases,
                stage.begin_cursor,
                commit_cursor,
                context,
            )
            .await
            .map_err(program_mutation_error)?;
        let mutation = ProgramAliasRegistryMutation {
            format: PROGRAM_ALIAS_REGISTRY_MUTATION_FORMAT,
            commit_cursor,
            stage,
            replacement,
        };
        mutation.validate()?;
        Ok(mutation)
    }

    pub async fn apply_program_alias_registry_finalization_replica(
        &self,
        mutation: &ProgramAliasRegistryMutation,
        context: ObjectMutationContext,
    ) -> Result<bool, ProgramStoreError> {
        mutation.validate()?;
        let target = stage_key_from_path(&mutation.stage.target)?;
        self.apply_alias_registry_replica_transition(
            mutation.stage.tenant_id,
            mutation.stage.bucket_id,
            &target,
            mutation.stage.expected.as_ref(),
            mutation.replacement.as_ref(),
            mutation.stage.begin_cursor,
            mutation.commit_cursor,
            context,
        )
        .await
        .map_err(program_mutation_error)
    }

    pub async fn persist_program_path_stage(
        &self,
        stage: &ProgramPathStage,
    ) -> Result<BlobRef, ProgramStoreError> {
        stage.validate()?;
        self.validate_program_path_policy(stage)?;
        self.require_program_stage_predecessor(stage)?;
        let encoded = stage.encoded()?;
        let reference = self
            .stage_blob(&encoded)
            .await
            .map_err(program_mutation_error)?;
        if reference != stage.blob_ref()? {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }
        Ok(reference)
    }

    pub async fn coordinate_program_path_finalization(
        &self,
        stage: ProgramPathStage,
        commit_cursor: u64,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedProgramPathFinalization, ProgramStoreError> {
        stage.validate()?;
        self.persist_program_path_stage(&stage).await?;
        loop {
            let commit_guard = self.lock_commit("distributed_program").await;
            self.validate_program_path_policy(&stage)?;
            let identity = stage_identity(&stage);
            let key = stage_key(&stage)?;
            self.require_committed_program_path_stage_reservation_locked(
                &stage,
                commit_cursor,
                context.serving_fence_term,
                context.active_placement_log_id,
            )
            .map_err(program_mutation_error)?;
            let current = self
                .head_by_storage_key(&identity.head_key(key.path()))
                .map_err(program_mutation_error)?;
            if current
                .as_ref()
                .is_some_and(|head| head.version == stage.version.id)
            {
                let stamp = current
                    .as_ref()
                    .and_then(|head| head.mutation_stamp)
                    .ok_or_else(|| ProgramStoreError::CommitCorruption {
                        cursor: commit_cursor,
                    })?;
                let mutation = replayed_program_path_mutation(stage.clone(), commit_cursor, stamp)?;
                return Ok(CoordinatedProgramPathFinalization {
                    mutation,
                    replayed: true,
                });
            }
            require_stage_head(&stage, current.as_ref())?;
            let source = self
                .local_watch_status()
                .map_err(|error| ProgramStoreError::Storage(error.to_string()))?;
            let offset = source.tail.checked_add(1).ok_or_else(|| {
                ProgramStoreError::Storage("local invalidation offset is exhausted".into())
            })?;
            let mutation = self.program_path_mutation(
                stage.clone(),
                commit_cursor,
                context,
                source.source_id,
                offset,
            )?;
            let attempt = self.apply_program_path_mutation_locked(&mutation, true);
            drop(commit_guard);
            match attempt {
                Ok(_) => {
                    return Ok(CoordinatedProgramPathFinalization {
                        mutation,
                        replayed: false,
                    });
                }
                Err(ProgramStoreError::SourceJournalCapacity) => {
                    self.wait_for_mutation_capacity().await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn apply_program_path_finalization_replica(
        &self,
        mutation: &ProgramPathMutation,
        context: ObjectMutationContext,
    ) -> Result<ReplicaProgramPathApplied, ProgramStoreError> {
        mutation.validate()?;
        self.persist_program_path_stage(&mutation.stage).await?;
        let _commit_guard = self.lock_commit("distributed_program").await;
        self.validate_program_path_policy(&mutation.stage)?;
        // Committed reservations follow the current recovery executor. The
        // sealed mutation stamp remains the original coordinator's immutable
        // replay provenance and may carry an older nomination.
        self.require_committed_program_path_stage_reservation_locked(
            &mutation.stage,
            mutation.commit_cursor,
            context.serving_fence_term,
            context.active_placement_log_id,
        )
        .map_err(program_mutation_error)?;
        self.apply_program_path_mutation_locked(mutation, false)
    }

    fn program_path_mutation(
        &self,
        stage: ProgramPathStage,
        commit_cursor: u64,
        context: ObjectMutationContext,
        source_id: SourceId,
        source_journal_position: u64,
    ) -> Result<ProgramPathMutation, ProgramStoreError> {
        let retention = stage_retention(&stage);
        let reference_deltas = self.current_program_path_reference_deltas(&stage)?;
        let accounting_transition = stage_accounting_transition(&stage, retention);
        let mut mutation = ProgramPathMutation {
            format: PROGRAM_PATH_MUTATION_FORMAT,
            commit_cursor,
            stamp: MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: stage.previous_version.as_ref().map(|version| version.id),
                program_commit_cursor: Some(commit_cursor),
                mutation_fingerprint: [0; 32],
                active_placement_log_id: context.active_placement_log_id,
                serving_fence_term: context.serving_fence_term,
                source_id,
                source_journal_position,
            },
            stage,
            reference_deltas,
            accounting_transition,
        };
        mutation.set_fingerprint()?;
        mutation.validate()?;
        Ok(mutation)
    }

    fn current_program_path_reference_deltas(
        &self,
        stage: &ProgramPathStage,
    ) -> Result<Vec<ReferenceDelta>, ProgramStoreError> {
        let mut deltas = reference_deltas(stage)?;
        if stage_retention(stage) == StoredVersionRetention::JournalPending
            && let Some(previous) = stage.previous_version.as_ref()
        {
            let identity = stage_identity(stage);
            let key = stage_key(stage)?;
            let predecessor_key = version_key(identity, &key, previous.id);
            if let Some(stored) = self
                .stored_version_by_key(&predecessor_key)
                .map_err(program_mutation_error)?
                && stored.retention == StoredVersionRetention::JournalReleased
                && let Some(blob) = stored.version.blob
            {
                deltas.push(ReferenceDelta { blob, change: -1 });
            }
        }
        if deltas.len() == 2 && deltas[0].blob == deltas[1].blob {
            deltas.clear();
        }
        Ok(deltas)
    }

    fn apply_program_path_mutation_locked(
        &self,
        mutation: &ProgramPathMutation,
        emit_source_change: bool,
    ) -> Result<ReplicaProgramPathApplied, ProgramStoreError> {
        mutation.validate()?;
        let stage = &mutation.stage;
        let identity = stage_identity(stage);
        let retention = stage_retention(stage);
        let key = stage_key(stage)?;
        let encoded_head_key = identity.head_key(key.path());
        let current = self
            .head_by_storage_key(&encoded_head_key)
            .map_err(program_mutation_error)?;
        if current.as_ref().is_some_and(|head| {
            head.version == stage.version.id
                && head.deleted == stage.version.deleted
                && head.mutation_stamp == Some(mutation.stamp)
        }) {
            return Ok(ReplicaProgramPathApplied {
                version: stage.version.id,
                replayed: true,
            });
        }
        if current
            .as_ref()
            .is_some_and(|head| head.version > stage.version.id)
        {
            return Ok(ReplicaProgramPathApplied {
                version: stage.version.id,
                replayed: true,
            });
        }
        require_stage_head(stage, current.as_ref())?;

        let mut batch = WriteBatch::default();
        let encoded_version_key = version_key(identity, &key, stage.version.id);
        let encoded_version =
            serde_json::to_vec(&StoredVersion::new(stage.version.clone(), retention))
                .map_err(program_storage_error)?;
        if let Some(existing) = self.raw_get(CF_VERSIONS, &encoded_version_key)?
            && existing != encoded_version
        {
            return Err(ProgramStoreError::CommitCorruption {
                cursor: mutation.commit_cursor,
            });
        }
        batch.put_cf(
            self.program_cf(CF_VERSIONS)?,
            encoded_version_key,
            encoded_version,
        );
        if let Some(current) = current.as_ref() {
            let predecessor_key = version_key(identity, &key, current.version);
            if let Some(mut stored) = self
                .stored_version_by_key(&predecessor_key)
                .map_err(program_mutation_error)?
            {
                // The sealed fingerprint chooses the reference delta. This
                // replica-local marker controls only descriptor cleanup: a
                // pending proof or journal record retires it later, while an
                // already released descriptor can disappear now.
                match stored.retention {
                    StoredVersionRetention::JournalPending
                        if retention == StoredVersionRetention::UserRetained =>
                    {
                        stored.retention = StoredVersionRetention::UserRetained;
                        batch.put_cf(
                            self.program_cf(CF_VERSIONS)?,
                            predecessor_key,
                            serde_json::to_vec(&stored).map_err(program_storage_error)?,
                        );
                    }
                    StoredVersionRetention::JournalReleased
                        if retention == StoredVersionRetention::UserRetained =>
                    {
                        stored.retention = StoredVersionRetention::UserRetained;
                        batch.put_cf(
                            self.program_cf(CF_VERSIONS)?,
                            predecessor_key,
                            serde_json::to_vec(&stored).map_err(program_storage_error)?,
                        );
                    }
                    StoredVersionRetention::JournalReleased => {
                        batch.delete_cf(self.program_cf(CF_VERSIONS)?, predecessor_key);
                    }
                    StoredVersionRetention::JournalPending
                    | StoredVersionRetention::UserRetained => {}
                }
            }
        }
        batch.put_cf(
            self.program_cf(CF_HEADS)?,
            encoded_head_key,
            serde_json::to_vec(&Head {
                version: stage.version.id,
                deleted: stage.version.deleted,
                mutation_stamp: Some(mutation.stamp),
            })
            .map_err(program_storage_error)?,
        );
        let high = self
            .read_program_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)?
            .map_or(stage.version.id, |current| current.max(stage.version.id));
        batch.put_cf(
            self.program_cf(CF_METADATA)?,
            VERSION_HIGH_WATERMARK_KEY,
            serde_json::to_vec(&high).map_err(program_storage_error)?,
        );
        let proof = program_reference_proof(mutation)?;
        self.stage_reference_proof_if_absent(&mut batch, &proof)
            .map_err(program_mutation_error)?;
        if emit_source_change {
            self.stage_local_changes(
                &mut batch,
                &[PendingLocalChange::ObjectHead {
                    identity,
                    exact_path: key.path().to_owned(),
                    path_version: stage.version.id,
                    deleted: stage.version.deleted,
                    program_commit_cursor: Some(mutation.commit_cursor),
                    reference_deltas: mutation.reference_deltas.clone(),
                    accounting_transition: Some(mutation.accounting_transition),
                    definition_transition: None,
                }],
                LocalReferenceEffects::Deferred,
            )
            .map_err(program_mutation_error)?;
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(program_storage_error)?;
        self.clock.observe(stage.version.id);
        if emit_source_change {
            self.notify_local_invalidations();
        }
        Ok(ReplicaProgramPathApplied {
            version: stage.version.id,
            replayed: false,
        })
    }

    fn validate_program_path_policy(
        &self,
        stage: &ProgramPathStage,
    ) -> Result<(), ProgramStoreError> {
        if is_program_definition_path(&stage.path.path) {
            return Err(ProgramStoreError::Immutable {
                path: stage.path.clone(),
            });
        }
        if stage.governance.policy.is_immutable(&stage.path.path)
            && (stage.version.deleted || !matches!(stage.expected, ObservedHead::NeverExisted))
        {
            return Err(ProgramStoreError::Immutable {
                path: stage.path.clone(),
            });
        }
        Ok(())
    }

    fn require_program_stage_predecessor(
        &self,
        stage: &ProgramPathStage,
    ) -> Result<(), ProgramStoreError> {
        let key = stage_key(stage)?;
        let current = self
            .head_by_storage_key(&stage_identity(stage).head_key(key.path()))
            .map_err(program_mutation_error)?;
        if current
            .as_ref()
            .is_some_and(|head| head.version >= stage.version.id)
        {
            return Ok(());
        }
        require_stage_head(stage, current.as_ref())
    }
}

fn stage_identity(stage: &ProgramPathStage) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(stage.tenant_id),
        bucket_id: BucketId(stage.bucket_id),
    }
}

fn stage_key(stage: &ProgramPathStage) -> Result<ObjectKey, ProgramStoreError> {
    ObjectKey::new(&stage.path.tenant, &stage.path.bucket, &stage.path.path)
        .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))
}

fn require_stage_head(
    stage: &ProgramPathStage,
    current: Option<&Head>,
) -> Result<(), ProgramStoreError> {
    if !head_matches(&stage.expected, current)? {
        return Err(ProgramStoreError::PreconditionFailed {
            path: stage.path.clone(),
            current: current.map(|head| head.version),
        });
    }
    Ok(())
}

fn reference_deltas(stage: &ProgramPathStage) -> Result<Vec<ReferenceDelta>, ProgramStoreError> {
    let new = version_blob_reference(&stage.version).map_err(program_mutation_error)?;
    let mut deltas = Vec::with_capacity(1);
    if let Some(new) = new {
        deltas.push(ReferenceDelta {
            blob: new,
            change: 1,
        });
    }
    Ok(deltas)
}

fn stage_retention(stage: &ProgramPathStage) -> StoredVersionRetention {
    match stage.governance.versioning {
        ObjectVersioning::Unversioned => StoredVersionRetention::JournalPending,
        ObjectVersioning::Enabled => StoredVersionRetention::UserRetained,
    }
}

/// A coordinator can select only the ordinary publication delta or the same
/// publication combined with retirement of a journal-released predecessor.
/// The latter descriptor is intentionally deleted by first application, so an
/// exact replay identifies the original choice through the stored fingerprint
/// rather than mutable predecessor state.
fn permitted_reference_delta_variants(
    stage: &ProgramPathStage,
) -> Result<Vec<Vec<ReferenceDelta>>, ProgramStoreError> {
    let ordinary = reference_deltas(stage)?;
    let mut variants = vec![ordinary.clone()];
    if stage_retention(stage) == StoredVersionRetention::JournalPending
        && let Some(previous_blob) = stage
            .previous_version
            .as_ref()
            .and_then(|version| version.blob.clone())
    {
        let mut released = ordinary;
        released.push(ReferenceDelta {
            blob: previous_blob,
            change: -1,
        });
        if released.len() == 2 && released[0].blob == released[1].blob {
            released.clear();
        }
        if !variants.contains(&released) {
            variants.push(released);
        }
    }
    Ok(variants)
}

fn replayed_program_path_mutation(
    stage: ProgramPathStage,
    commit_cursor: u64,
    stamp: MutationStamp,
) -> Result<ProgramPathMutation, ProgramStoreError> {
    let accounting_transition = stage_accounting_transition(&stage, stage_retention(&stage));
    let mut matched = None;
    for reference_deltas in permitted_reference_delta_variants(&stage)? {
        let candidate = ProgramPathMutation {
            format: PROGRAM_PATH_MUTATION_FORMAT,
            commit_cursor,
            stage: stage.clone(),
            stamp,
            reference_deltas,
            accounting_transition,
        };
        if candidate.computed_fingerprint()? == stamp.mutation_fingerprint {
            candidate.validate()?;
            if matched.replace(candidate).is_some() {
                return Err(ProgramStoreError::CommitCorruption {
                    cursor: commit_cursor,
                });
            }
        }
    }
    matched.ok_or(ProgramStoreError::CommitCorruption {
        cursor: commit_cursor,
    })
}

fn program_reference_proof(
    mutation: &ProgramPathMutation,
) -> Result<ReferenceProof, ProgramStoreError> {
    mutation.validate()?;
    Ok(ReferenceProof::new(
        mutation.stamp.source_id,
        mutation.stamp.mutation_fingerprint,
        crate::LocalChange::object_head_with_program_cursor(
            mutation.stamp.source_journal_position,
            mutation.stage.tenant_id,
            mutation.stage.bucket_id,
            mutation.stage.path.path.clone(),
            mutation.stage.version.id,
            mutation.stage.version.deleted,
            Some(mutation.commit_cursor),
            mutation.reference_deltas.clone(),
            Some(mutation.accounting_transition),
            None,
        ),
        ReferenceProofMutation::ProgramPath(mutation.clone()),
    ))
}

fn stage_accounting_transition(
    stage: &ProgramPathStage,
    retention: StoredVersionRetention,
) -> AccountingHeadTransition {
    let previous_live_length = stage
        .previous_version
        .as_ref()
        .and_then(live_version_length);
    AccountingHeadTransition::new(
        previous_live_length,
        live_version_length(&stage.version),
        if retention == StoredVersionRetention::JournalPending {
            previous_live_length.unwrap_or(0)
        } else {
            0
        },
    )
}

/// Build one path stage from a verified complete prepared record and the
/// authoritative snapshot used by the evaluator.
pub fn path_stage_from_prepared(
    prepared: &PreparedProgramBundle,
    record: &PreparedProgramRecord,
    write: &PreparedVersionWrite,
    begin_cursor: u64,
) -> Result<ProgramPathStage, ProgramStoreError> {
    if record.authority() != prepared.authority
        || record.participant_manifest_hash()? != prepared.participant_manifest_hash
        || !record.writes().contains(write)
    {
        return Err(ProgramStoreError::PreparedBundleMismatch);
    }
    let (tenant_id, bucket_id, governance) = {
        let participant = record
            .participant_manifest()
            .objects
            .iter()
            .find(|participant| participant.path == *write.path())
            .ok_or_else(|| {
                ProgramStoreError::InvalidBundle(
                    "prepared write has no matching object participant".into(),
                )
            })?;
        if participant.condition.observed_head().as_ref() != Some(write.expected()) {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared write disagrees with its participant condition".into(),
            ));
        }
        let governance = record
            .participant_manifest()
            .governance
            .iter()
            .find(|governance| {
                governance.tenant == write.path().tenant
                    && governance.bucket == write.path().bucket
                    && governance.tenant_id == participant.tenant_id
                    && governance.bucket_id == participant.bucket_id
            })
            .cloned()
            .ok_or_else(|| {
                ProgramStoreError::InvalidBundle(
                    "prepared write has no matching authoritative governance".into(),
                )
            })?;
        (participant.tenant_id, participant.bucket_id, governance)
    };
    let stage = ProgramPathStage {
        format: PROGRAM_PATH_STAGE_FORMAT,
        begin_cursor,
        bundle_hash: prepared.bundle.hash,
        program_hash: prepared.program_hash,
        authority: prepared.authority,
        participant_manifest_hash: prepared.participant_manifest_hash,
        tenant_id,
        bucket_id,
        governance,
        path: write.path.clone(),
        expected: write.expected.clone(),
        previous_version: write.previous_version.clone(),
        version: write.version.clone(),
    };
    stage.validate()?;
    Ok(stage)
}

pub fn alias_registry_stages_from_prepared(
    prepared: &PreparedProgramBundle,
    record: &PreparedProgramRecord,
    begin_cursor: u64,
) -> Result<Vec<ProgramAliasRegistryStage>, ProgramStoreError> {
    record
        .alias_registry_writes()?
        .into_iter()
        .map(|(target, expected, replacement_aliases)| {
            let stage = ProgramAliasRegistryStage {
                format: PROGRAM_ALIAS_REGISTRY_STAGE_FORMAT,
                begin_cursor,
                bundle_hash: prepared.bundle.hash,
                program_hash: prepared.program_hash,
                authority: prepared.authority,
                participant_manifest_hash: prepared.participant_manifest_hash,
                tenant_id: target.tenant_id,
                bucket_id: target.bucket_id,
                target: target.path.clone(),
                expected: expected.cloned(),
                replacement_aliases: replacement_aliases.to_vec(),
            };
            stage.validate()?;
            Ok(stage)
        })
        .collect()
}

fn valid_alias_replacement(canonical_path: &str, aliases: &[String]) -> bool {
    aliases.is_empty()
        || crate::ObjectAliasRegistry {
            format: crate::OBJECT_ALIAS_REGISTRY_FORMAT,
            revision: 1,
            aliases: aliases.to_vec(),
            program_commit_cursor: Some(1),
        }
        .validate(canonical_path)
        .is_ok()
}

fn stage_key_from_path(path: &ObjectPath) -> Result<ObjectKey, ProgramStoreError> {
    ObjectKey::new(&path.tenant, &path.bucket, &path.path)
        .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))
}
