//! Checkpoint-coupled retirement of exact source versions.

use super::mutation_helpers::exact_version_key;
use super::*;
use crate::{ReferenceProof, ReferenceProofMutation};

const STORED_VERSION_FORMAT: u16 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoredVersionRetention {
    JournalPending,
    JournalReleased,
    UserRetained,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredVersion {
    format: u16,
    pub(crate) retention: StoredVersionRetention,
    pub(crate) version: Version,
}

impl StoredVersion {
    pub(crate) fn new(version: Version, retention: StoredVersionRetention) -> Self {
        Self {
            format: STORED_VERSION_FORMAT,
            retention,
            version,
        }
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, MutationError> {
        let stored: Self = serde_json::from_slice(encoded).map_err(storage_error)?;
        if stored.format != STORED_VERSION_FORMAT {
            return Err(MutationError::Storage(format!(
                "unsupported stored-version format {}",
                stored.format
            )));
        }
        Ok(stored)
    }
}

impl Store {
    pub(crate) fn version_retention_for_bucket(
        &self,
        identity: BucketIdentity,
    ) -> Result<StoredVersionRetention, MutationError> {
        Ok(match self.bucket_versioning_by_key(&identity.encode())? {
            ObjectVersioning::Unversioned => StoredVersionRetention::JournalPending,
            ObjectVersioning::Enabled => StoredVersionRetention::UserRetained,
        })
    }

    pub(crate) fn stored_version_by_key(
        &self,
        key: &[u8],
    ) -> Result<Option<StoredVersion>, MutationError> {
        self.db
            .get_cf(self.cf(CF_VERSIONS)?, key)
            .map_err(storage_error)?
            .map(|encoded| StoredVersion::decode(&encoded))
            .transpose()
    }

    pub(super) fn stage_pruned_change_version_retirement(
        &self,
        batch: &mut WriteBatch,
        retired_keys: &mut BTreeSet<Vec<u8>>,
        change: &LocalChange,
    ) -> Result<Option<PendingLocalChange>, MutationError> {
        let LocalChange::ObjectHead(change) = change else {
            return Ok(None);
        };
        let Some((_resulting_head_version, reference_delta)) = self
            .stage_checkpointed_source_version_retirement(
                batch,
                retired_keys,
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change.path_version,
            )?
        else {
            return Ok(None);
        };
        let retained_bytes_removed = reference_delta
            .as_ref()
            .map_or(0, |delta| delta.blob.length);
        Ok(Some(PendingLocalChange::ContentLifecycleChanged {
            blob_identity: exact_version_key(
                BucketIdentity {
                    tenant_id: TenantId(change.tenant_id),
                    bucket_id: BucketId(change.bucket_id),
                },
                &change.exact_path,
                change.path_version,
            ),
            revision: change.path_version.0,
            reference_deltas: reference_delta.into_iter().collect(),
            accounting_transition: Some(crate::ContentAccountingTransition {
                tenant_id: change.tenant_id,
                bucket_id: change.bucket_id,
                exact_path: change.exact_path.clone(),
                retained_bytes_removed,
            }),
        }))
    }

    pub(super) fn stage_pruned_reference_proof_version_retirement(
        &self,
        batch: &mut WriteBatch,
        retired_keys: &mut BTreeSet<Vec<u8>>,
        proof: &ReferenceProof,
    ) -> Result<bool, MutationError> {
        if proof.source_id
            == self
                .local_watch_status()
                .map_err(|error| MutationError::Storage(error.to_string()))?
                .source_id
        {
            // The source journal emits the authoritative delayed reference
            // release. Its local proof must not consume that descriptor first.
            return Ok(false);
        }
        let (tenant_id, bucket_id, exact_path, version_id) = match &proof.mutation {
            ReferenceProofMutation::Object(mutation) => (
                mutation.tenant_id,
                mutation.bucket_id,
                mutation.exact_path.as_str(),
                mutation.version.id,
            ),
            ReferenceProofMutation::ProgramPath(mutation) => (
                mutation.stage.tenant_id,
                mutation.stage.bucket_id,
                mutation.stage.path.path.as_str(),
                mutation.stage.version.id,
            ),
            ReferenceProofMutation::RetainedVersionDelete(_) => return Ok(false),
        };
        self.stage_checkpointed_source_version_retirement(
            batch,
            retired_keys,
            tenant_id,
            bucket_id,
            exact_path,
            version_id,
        )
        .map(|retired| retired.is_some())
    }

    /// Retires one event-owned immutable version only after the caller has
    /// proven that the source event naming it is below the durable retention
    /// floor. The descriptor, payload reference, and proof/journal deletion
    /// must share the caller's synced RocksDB batch.
    pub(super) fn stage_checkpointed_source_version_retirement(
        &self,
        batch: &mut WriteBatch,
        retired_keys: &mut BTreeSet<Vec<u8>>,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        version_id: VersionId,
    ) -> Result<Option<(VersionId, Option<ReferenceDelta>)>, MutationError> {
        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let version_key = exact_version_key(identity, exact_path, version_id);
        if retired_keys.contains(&version_key) {
            return Ok(None);
        }
        let head_key = identity.head_key(exact_path);
        let head = self.head_by_storage_key(&head_key)?;
        let Some(stored) = self.stored_version_by_key(&version_key)? else {
            return Ok(None);
        };
        if stored.retention != StoredVersionRetention::JournalPending {
            return Ok(None);
        }
        let version = stored.version;
        if version.id != version_id {
            return Err(MutationError::Storage(
                "checkpoint-retained version key and descriptor disagree".into(),
            ));
        }
        if head.as_ref().is_some_and(|head| head.version == version_id) {
            batch.put_cf(
                self.cf(CF_VERSIONS)?,
                &version_key,
                serde_json::to_vec(&StoredVersion::new(
                    version,
                    StoredVersionRetention::JournalReleased,
                ))
                .map_err(storage_error)?,
            );
            return Ok(None);
        }
        let reference_delta = version
            .blob
            .clone()
            .map(|blob| ReferenceDelta { blob, change: -1 });
        batch.delete_cf(self.cf(CF_VERSIONS)?, &version_key);
        retired_keys.insert(version_key);
        let resulting_head = head.ok_or_else(|| {
            MutationError::Storage("checkpoint-retained descriptor has no current head".into())
        })?;
        Ok(Some((resulting_head.version, reference_delta)))
    }
}
