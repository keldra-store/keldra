//! Typed boundaries between publication preparation, durable submission, and
//! exact receipt application. Keeping these phases separate lets the router
//! cohort requests across indexes without weakening per-index ordering.

use std::time::SystemTime;

use keldra_index::{IndexError, v4::ArtifactPackReference};
use keldra_store::{BlobRef, DefinitionKind, VersionId};
use tonic::Status;

use crate::index_service::{StoredIndexDefinition, definition_path};

use super::{
    CommitManifestReference, CompletedSegmentPacks, DefinitionVersionGuard,
    DerivedArtifactAdmission, IndexArtifactOutcome, IndexArtifactPublish, IndexCommitManifest,
    IndexCurrentPointer, StagedIndexPack, artifact_path, current_path, deduplicate_staged_packs,
    manifest_path, publish_command,
};

pub(crate) struct PreparedPackPublication {
    index_id: u64,
    base_packs: Vec<ArtifactPackReference>,
    staged: Vec<StagedIndexPack>,
    outcome_ordinals: Vec<usize>,
    requests: Vec<IndexArtifactPublish>,
}

impl PreparedPackPublication {
    pub(super) fn new(
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        admission: DerivedArtifactAdmission,
        packs: CompletedSegmentPacks,
    ) -> Result<Self, IndexError> {
        let (unique, outcome_ordinals) = deduplicate_staged_packs(&packs.staged)?;
        let requests = unique
            .into_iter()
            .map(|pack_index| {
                let pack = &packs.staged[pack_index];
                let path = artifact_path(definition.index_id, pack.blob.hash);
                IndexArtifactPublish {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    index_id: definition.index_id,
                    exact_path: path.clone(),
                    blob: pack.blob.clone(),
                    expected_version: None,
                    command_id: publish_command(definition.index_id, &path, &pack.blob, None),
                    definition_guard: None,
                    definition_intent: None,
                    admission,
                }
            })
            .collect();
        Ok(Self {
            index_id: definition.index_id,
            base_packs: packs.base_packs,
            staged: packs.staged,
            outcome_ordinals,
            requests,
        })
    }

    pub(crate) fn requests(&self) -> &[IndexArtifactPublish] {
        &self.requests
    }

    /// No pack reference escapes until every distinct immutable request has an
    /// exact receipt. Failed items can therefore be replayed without
    /// manufacturing a partially durable segment descriptor.
    pub(crate) fn apply(
        self,
        outcomes: &[IndexArtifactOutcome],
    ) -> Result<Vec<ArtifactPackReference>, IndexError> {
        if outcomes.len() != self.requests.len() {
            return Err(IndexError::InvalidFormat(
                "grouped pack outcome count differs from staged pack count",
            ));
        }
        let mut references = self.base_packs;
        references.reserve(self.staged.len());
        for (pack, outcome_ordinal) in self.staged.into_iter().zip(self.outcome_ordinals) {
            let outcome = outcomes
                .get(outcome_ordinal)
                .ok_or(IndexError::InvalidFormat(
                    "grouped pack outcome ordinal is missing",
                ))?;
            references.push(ArtifactPackReference::new(
                self.index_id,
                artifact_path(self.index_id, pack.blob.hash),
                outcome.version.0,
                pack.blob.hash,
                pack.blob.length,
            )?);
        }
        Ok(references)
    }
}

pub(crate) struct PreparedManifestPublication {
    pub(crate) manifest: IndexCommitManifest,
    pub(crate) blob: BlobRef,
    request: IndexArtifactPublish,
}

impl PreparedManifestPublication {
    pub(crate) fn new(
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        manifest: IndexCommitManifest,
        blob: BlobRef,
        admission: DerivedArtifactAdmission,
    ) -> Self {
        let path = manifest_path(definition.index_id, blob.hash);
        let request = IndexArtifactPublish {
            storage_tenant: definition.tenant.clone(),
            bucket: definition.bucket.clone(),
            tenant_id,
            bucket_id,
            index_id: definition.index_id,
            exact_path: path.clone(),
            blob: blob.clone(),
            expected_version: None,
            command_id: publish_command(definition.index_id, &path, &blob, None),
            definition_guard: None,
            definition_intent: None,
            admission,
        };
        Self {
            manifest,
            blob,
            request,
        }
    }

    pub(crate) fn request(&self) -> &IndexArtifactPublish {
        &self.request
    }

    pub(crate) fn apply(
        self,
        outcome: IndexArtifactOutcome,
        published_at: SystemTime,
    ) -> Result<PublishedManifest, Status> {
        let reference =
            CommitManifestReference::new(&self.manifest, self.blob, outcome.version, published_at)
                .map_err(super::commit_view_status)?;
        Ok(PublishedManifest {
            manifest: self.manifest,
            reference,
        })
    }
}

pub(crate) struct PublishedManifest {
    pub(crate) manifest: IndexCommitManifest,
    pub(crate) reference: CommitManifestReference,
}

pub(crate) struct PreparedCurrentPointerPublication {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) manifest: IndexCommitManifest,
    request: IndexArtifactPublish,
}

impl PreparedCurrentPointerPublication {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        definition_version: u64,
        pointer: IndexCurrentPointer,
        manifest: IndexCommitManifest,
        blob: BlobRef,
        expected_version: Option<VersionId>,
        admission: DerivedArtifactAdmission,
    ) -> Result<Self, Status> {
        let path = current_path(definition.index_id);
        let request = IndexArtifactPublish {
            storage_tenant: definition.tenant.clone(),
            bucket: definition.bucket.clone(),
            tenant_id,
            bucket_id,
            index_id: definition.index_id,
            exact_path: path.clone(),
            blob: blob.clone(),
            expected_version,
            command_id: publish_command(definition.index_id, &path, &blob, expected_version),
            definition_guard: Some(DefinitionVersionGuard {
                kind: DefinitionKind::Index,
                exact_path: definition_path(&definition.name)?,
                expected_version: VersionId(definition_version),
            }),
            definition_intent: None,
            admission,
        };
        Ok(Self {
            pointer,
            manifest,
            request,
        })
    }

    pub(crate) fn request(&self) -> &IndexArtifactPublish {
        &self.request
    }

    pub(crate) fn apply(self, outcome: IndexArtifactOutcome) -> super::CommittedIndexView {
        super::CommittedIndexView {
            pointer: self.pointer,
            current_object_version: outcome.version,
            manifest: self.manifest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn staged(hash: u8) -> StagedIndexPack {
        StagedIndexPack {
            blob: BlobRef {
                hash: [hash; 32],
                length: 128,
            },
            component_count: 1,
        }
    }

    fn prepared(staged: Vec<StagedIndexPack>) -> PreparedPackPublication {
        let requests = staged
            .iter()
            .map(|pack| IndexArtifactPublish {
                storage_tenant: "tenant".into(),
                bucket: "bucket".into(),
                tenant_id: 1,
                bucket_id: 2,
                index_id: 7,
                exact_path: artifact_path(7, pack.blob.hash),
                blob: pack.blob.clone(),
                expected_version: None,
                command_id: "test".into(),
                definition_guard: None,
                definition_intent: None,
                admission: DerivedArtifactAdmission::Bounded,
            })
            .collect::<Vec<_>>();
        PreparedPackPublication {
            index_id: 7,
            base_packs: Vec::new(),
            outcome_ordinals: (0..staged.len()).collect(),
            staged,
            requests,
        }
    }

    #[test]
    fn pack_references_require_every_unique_receipt() {
        let prepared = prepared(vec![staged(1), staged(2)]);
        assert_eq!(prepared.requests().len(), 2);
        assert!(
            prepared
                .apply(&[IndexArtifactOutcome {
                    version: VersionId(3),
                    replayed: false,
                }])
                .is_err()
        );
    }

    #[test]
    fn replayed_receipts_resolve_lost_responses_exactly() {
        let prepared = prepared(vec![staged(1)]);
        let references = prepared
            .apply(&[IndexArtifactOutcome {
                version: VersionId(9),
                replayed: true,
            }])
            .unwrap();
        assert_eq!(references[0].object_version, 9);
    }
}
