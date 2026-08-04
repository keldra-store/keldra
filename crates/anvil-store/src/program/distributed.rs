use anvil_atomic_program::{ObjectPath, ObservedHead};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

use super::*;
use crate::key::{BucketId, BucketIdentity, TenantId};
use crate::store::{
    CF_HEADS, CF_METADATA, CF_VERSIONS, PendingLocalChange, VERSION_HIGH_WATERMARK_KEY, version_key,
};
use crate::{
    MUTATION_STAMP_FORMAT, MutationStamp, ObjectMutationContext, ReferenceProof, SourceId,
};

pub const PROGRAM_PATH_STAGE_FORMAT: u16 = 1;
pub const PROGRAM_PATH_MUTATION_FORMAT: u16 = 1;

/// One immutable, path-scoped slice of the ordinary prepared bundle.
///
/// Its canonical bytes are sealed through the normal blob plane on every
/// required metadata replica before Raft commits. There is intentionally no
/// stage column family, lookup index, receipt, or cleanup protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramPathStage {
    pub format: u16,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub tenant_id: u64,
    pub bucket_id: u64,
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
        if self.format != PROGRAM_PATH_STAGE_FORMAT
            || self.bundle_hash.0 == [0; 32]
            || self.program_hash.0 == [0; 32]
            || self.tenant_id == 0
            || self.bucket_id == 0
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
    pub retire_predecessor: bool,
    pub stamp: MutationStamp,
    pub reference_deltas: Vec<ReferenceDelta>,
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
        Ok(tagged_hash(b"anvil.program-path-mutation.v1", &encoded))
    }

    pub fn validate(&self) -> Result<(), ProgramStoreError> {
        self.stage.validate()?;
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

impl StoredPreparedBundle {
    pub fn decode_distributed(
        bytes: &[u8],
        bundle: PreparedBundleRef,
        bundle_hash: PreparedBundleHash,
        program_hash: ProgramHash,
    ) -> Result<Self, ProgramStoreError> {
        if bytes.len() as u64 != bundle.length
            || bundle.hash != bundle_hash.0
            || *blake3::hash(bytes).as_bytes() != bundle.hash
        {
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

    pub fn source_bundle_hash(&self) -> PreparedBundleHash {
        self.source_bundle_hash
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
        let _commit_guard = self.commit_lock.lock().await;
        self.validate_program_path_policy(&stage)?;
        let identity = stage_identity(&stage);
        let key = stage_key(&stage)?;
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
            let mut mutation = self.program_path_mutation(
                stage,
                commit_cursor,
                context,
                stamp.source_id,
                stamp.source_journal_position,
            )?;
            mutation.stamp = stamp;
            mutation.validate()?;
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
        let mutation =
            self.program_path_mutation(stage, commit_cursor, context, source.source_id, offset)?;
        self.apply_program_path_mutation_locked(&mutation, true)?;
        Ok(CoordinatedProgramPathFinalization {
            mutation,
            replayed: false,
        })
    }

    pub async fn apply_program_path_finalization_replica(
        &self,
        mutation: &ProgramPathMutation,
    ) -> Result<ReplicaProgramPathApplied, ProgramStoreError> {
        mutation.validate()?;
        self.persist_program_path_stage(&mutation.stage).await?;
        let _commit_guard = self.commit_lock.lock().await;
        self.validate_program_path_policy(&mutation.stage)?;
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
        let identity = stage_identity(&stage);
        let retire_predecessor = self
            .bucket_versioning_by_key(&identity.encode())
            .map_err(program_mutation_error)?
            == ObjectVersioning::Unversioned
            && stage.previous_version.is_some();
        let reference_deltas = reference_deltas(&stage, retire_predecessor)?;
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
            retire_predecessor,
            reference_deltas,
        };
        mutation.set_fingerprint()?;
        mutation.validate()?;
        Ok(mutation)
    }

    fn apply_program_path_mutation_locked(
        &self,
        mutation: &ProgramPathMutation,
        emit_source_change: bool,
    ) -> Result<ReplicaProgramPathApplied, ProgramStoreError> {
        mutation.validate()?;
        let stage = &mutation.stage;
        let identity = stage_identity(stage);
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
        if let Some(existing) = self.raw_get(CF_VERSIONS, &encoded_version_key)?
            && existing != serde_json::to_vec(&stage.version).map_err(program_storage_error)?
        {
            return Err(ProgramStoreError::CommitCorruption {
                cursor: mutation.commit_cursor,
            });
        }
        if mutation.retire_predecessor {
            let predecessor = stage.previous_version.as_ref().ok_or_else(|| {
                ProgramStoreError::InvalidBundle("retired predecessor is absent".into())
            })?;
            batch.delete_cf(
                self.program_cf(CF_VERSIONS)?,
                version_key(identity, &key, predecessor.id),
            );
        }
        batch.put_cf(
            self.program_cf(CF_VERSIONS)?,
            encoded_version_key,
            serde_json::to_vec(&stage.version).map_err(program_storage_error)?,
        );
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
        let proof = program_reference_proof(mutation);
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
                    reference_deltas: mutation.reference_deltas.clone(),
                    accounting_transition: Some(stage_accounting_transition(stage)),
                }],
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
        let policy = self
            .bucket_policy_by_key(&stage_identity(stage).encode())
            .map_err(program_mutation_error)?
            .unwrap_or_default();
        if !policy.is_program_only(&stage.path.path) {
            return Err(ProgramStoreError::ProgramPolicy {
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

fn reference_deltas(
    stage: &ProgramPathStage,
    retire_predecessor: bool,
) -> Result<Vec<ReferenceDelta>, ProgramStoreError> {
    let old = stage
        .previous_version
        .as_ref()
        .map(version_blob_reference)
        .transpose()
        .map_err(program_mutation_error)?
        .flatten();
    let new = version_blob_reference(&stage.version).map_err(program_mutation_error)?;
    let mut deltas = Vec::with_capacity(2);
    if retire_predecessor
        && old != new
        && let Some(old) = old.as_ref()
    {
        deltas.push(ReferenceDelta {
            blob: old.clone(),
            change: -1,
        });
    }
    if old != new
        && let Some(new) = new
    {
        deltas.push(ReferenceDelta {
            blob: new,
            change: 1,
        });
    }
    Ok(deltas)
}

fn program_reference_proof(mutation: &ProgramPathMutation) -> ReferenceProof {
    ReferenceProof::new(
        mutation.stamp.source_id,
        mutation.stamp.mutation_fingerprint,
        crate::LocalChange::object_head(
            mutation.stamp.source_journal_position,
            mutation.stage.tenant_id,
            mutation.stage.bucket_id,
            mutation.stage.path.path.clone(),
            mutation.stage.version.id,
            mutation.stage.version.deleted,
            mutation.reference_deltas.clone(),
            Some(stage_accounting_transition(&mutation.stage)),
        ),
    )
}

fn stage_accounting_transition(stage: &ProgramPathStage) -> AccountingHeadTransition {
    AccountingHeadTransition::new(
        stage
            .previous_version
            .as_ref()
            .and_then(|version| version.blob.as_ref().map(|blob| blob.length)),
        stage.version.blob.as_ref().map(|blob| blob.length),
    )
}

/// Build one path stage from a verified complete prepared record and the
/// authoritative snapshot used by the evaluator.
pub fn path_stage_from_prepared(
    prepared: &PreparedProgramBundle,
    write: &PreparedVersionWrite,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<ProgramPathStage, ProgramStoreError> {
    let stage = ProgramPathStage {
        format: PROGRAM_PATH_STAGE_FORMAT,
        bundle_hash: prepared.hash,
        program_hash: prepared.program_hash,
        tenant_id,
        bucket_id,
        path: write.path.clone(),
        expected: write.expected.clone(),
        previous_version: write.previous_version.clone(),
        version: write.version.clone(),
    };
    stage.validate()?;
    Ok(stage)
}
