//! Typed boundaries between publication preparation, durable submission, and
//! exact receipt application. Keeping these phases separate lets the router
//! cohort requests across indexes without weakening per-index ordering.

use std::time::{Duration, Instant, SystemTime};

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
use crate::index_runtime::manager::publication_cohort::{
    IndexPublicationCohorts, PublicationCohortClass,
};
use crate::index_runtime::publication::MAX_INDEX_ARTIFACT_BATCH_BYTES;

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

    pub(crate) fn logical_artifact_bytes(&self) -> Result<u64, IndexError> {
        self.requests.iter().try_fold(0_u64, |bytes, request| {
            bytes
                .checked_add(request.blob.length)
                .ok_or(IndexError::OffsetOverflow)
        })
    }

    pub(crate) fn staged_pack_count(&self) -> u64 {
        self.staged.len() as u64
    }

    pub(crate) fn staged_component_count(&self) -> Result<u64, IndexError> {
        self.staged.iter().try_fold(0_u64, |count, pack| {
            count
                .checked_add(pack.component_count)
                .ok_or(IndexError::OffsetOverflow)
        })
    }

    pub(crate) fn resident_bytes(&self) -> Result<usize, IndexError> {
        let mut bytes = std::mem::size_of::<Self>()
            .checked_add(
                self.base_packs
                    .capacity()
                    .checked_mul(std::mem::size_of::<ArtifactPackReference>())
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    self.staged
                        .capacity()
                        .checked_mul(std::mem::size_of::<StagedIndexPack>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.outcome_ordinals
                        .capacity()
                        .checked_mul(std::mem::size_of::<usize>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.requests
                        .capacity()
                        .checked_mul(std::mem::size_of::<IndexArtifactPublish>())?,
                )
            })
            .ok_or(IndexError::OffsetOverflow)?;
        for pack in &self.base_packs {
            bytes = bytes
                .checked_add(pack.path.capacity())
                .ok_or(IndexError::OffsetOverflow)?;
        }
        for request in &self.requests {
            bytes = bytes
                .checked_add(request.storage_tenant.capacity())
                .and_then(|bytes| bytes.checked_add(request.bucket.capacity()))
                .and_then(|bytes| bytes.checked_add(request.exact_path.capacity()))
                .and_then(|bytes| bytes.checked_add(request.command_id.capacity()))
                .ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(bytes)
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

pub(crate) struct PublishedPreparedPacks {
    pub(crate) references: Vec<Vec<ArtifactPackReference>>,
    pub(crate) logical_artifacts: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) cohort_calls: u64,
    pub(crate) cohort_duration: Duration,
}

pub(crate) async fn publish_prepared_packs(
    cohorts: &IndexPublicationCohorts,
    class: PublicationCohortClass,
    prepared: Vec<PreparedPackPublication>,
) -> Result<PublishedPreparedPacks, IndexError> {
    let mut requests = Vec::new();
    let mut ranges = Vec::with_capacity(prepared.len());
    let mut logical_bytes = 0u64;
    for publication in &prepared {
        let start = requests.len();
        logical_bytes = logical_bytes
            .checked_add(publication.logical_artifact_bytes()?)
            .ok_or(IndexError::OffsetOverflow)?;
        requests.extend_from_slice(publication.requests());
        ranges.push(start..requests.len());
    }
    let mut outcomes = vec![None; requests.len()];
    let started = Instant::now();
    let mut cohort_calls = 0u64;
    let mut pending = aggregate_pending_ordinals(&requests)?;
    while !pending.is_empty() {
        let submitted = pending
            .iter()
            .map(|&ordinal| requests[ordinal].clone())
            .collect();
        cohort_calls = cohort_calls
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        let published = match cohorts.publish_packs(submitted, class).await {
            Err(error) if retryable_pack_publish_status(&error) => {
                tracing::debug!(%error, "retrying retained immutable index pack cohort");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(error) => return Err(IndexError::Io(error.to_string())),
            Ok(published) if published.len() == pending.len() => published,
            Ok(_) => {
                return Err(IndexError::InvalidFormat(
                    "grouped pack outcome count differs from submitted pack count",
                ));
            }
        };
        pending = retain_pack_outcomes(pending, published, &mut outcomes)?;
        if !pending.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let outcomes =
        outcomes
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(IndexError::InvalidFormat(
                "grouped pack publication left an unresolved receipt",
            ))?;
    let logical_artifacts =
        u64::try_from(outcomes.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let references = apply_prepared_outcomes(prepared, &ranges, &outcomes)?;
    Ok(PublishedPreparedPacks {
        references,
        logical_artifacts,
        logical_bytes,
        cohort_calls,
        cohort_duration: started.elapsed(),
    })
}

fn retain_pack_outcomes(
    ordinals: Vec<usize>,
    published: Vec<Result<IndexArtifactOutcome, Status>>,
    outcomes: &mut [Option<IndexArtifactOutcome>],
) -> Result<Vec<usize>, IndexError> {
    if ordinals.len() != published.len() {
        return Err(IndexError::InvalidFormat(
            "grouped pack outcome count differs from submitted pack count",
        ));
    }
    let mut retry = Vec::new();
    for (ordinal, result) in ordinals.into_iter().zip(published) {
        match result {
            Ok(outcome) => outcomes[ordinal] = Some(outcome),
            Err(error) if retryable_pack_publish_status(&error) => retry.push(ordinal),
            Err(error) => return Err(IndexError::Io(error.to_string())),
        }
    }
    Ok(retry)
}

fn apply_prepared_outcomes(
    prepared: Vec<PreparedPackPublication>,
    ranges: &[std::ops::Range<usize>],
    outcomes: &[IndexArtifactOutcome],
) -> Result<Vec<Vec<ArtifactPackReference>>, IndexError> {
    if prepared.len() != ranges.len() {
        return Err(IndexError::InvalidFormat(
            "prepared publication mapping count differs from segment count",
        ));
    }
    prepared
        .into_iter()
        .zip(ranges)
        .map(|(publication, range)| publication.apply(&outcomes[range.clone()]))
        .collect()
}

fn aggregate_pending_ordinals(requests: &[IndexArtifactPublish]) -> Result<Vec<usize>, IndexError> {
    for request in requests {
        if request.blob.length > MAX_INDEX_ARTIFACT_BATCH_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: usize::try_from(request.blob.length).unwrap_or(usize::MAX),
                limit: usize::try_from(MAX_INDEX_ARTIFACT_BATCH_BYTES).unwrap_or(usize::MAX),
            });
        }
    }
    Ok((0..requests.len()).collect())
}

pub(super) fn retryable_pack_publish_status(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
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

    fn outcome(version: u64) -> IndexArtifactOutcome {
        IndexArtifactOutcome {
            version: VersionId(version),
            replayed: false,
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

    #[test]
    fn outcome_slices_preserve_two_empty_one_mapping() {
        let publications = vec![
            prepared(vec![staged(1), staged(2)]),
            prepared(Vec::new()),
            prepared(vec![staged(3)]),
        ];
        let references = apply_prepared_outcomes(
            publications,
            &[0..2, 2..2, 2..3],
            &[outcome(11), outcome(12), outcome(13)],
        )
        .unwrap();
        assert_eq!(references.len(), 3);
        assert_eq!(references[0][0].object_version, 11);
        assert_eq!(references[0][1].object_version, 12);
        assert!(references[1].is_empty());
        assert_eq!(references[2][0].object_version, 13);
    }

    #[test]
    fn aggregate_candidate_keeps_one_path_and_rejects_individual_oversize() {
        let item_requests = (0..=1_000)
            .map(|ordinal| IndexArtifactPublish {
                storage_tenant: "tenant".into(),
                bucket: "bucket".into(),
                tenant_id: 1,
                bucket_id: 2,
                index_id: 7,
                exact_path: format!("artifact/{ordinal}"),
                blob: BlobRef {
                    hash: [(ordinal % 251) as u8; 32],
                    length: 1,
                },
                expected_version: None,
                command_id: format!("command-{ordinal}"),
                definition_guard: None,
                definition_intent: None,
                admission: DerivedArtifactAdmission::Bounded,
            })
            .collect::<Vec<_>>();
        let pending = aggregate_pending_ordinals(&item_requests).unwrap();
        assert_eq!(pending, (0..=1_000).collect::<Vec<_>>());

        let mut oversized = item_requests[..1].to_vec();
        oversized[0].blob.length = MAX_INDEX_ARTIFACT_BATCH_BYTES + 1;
        assert!(aggregate_pending_ordinals(&oversized).is_err());
    }

    #[test]
    fn transient_partial_outcome_retries_only_unresolved_ordinal() {
        let mut outcomes = vec![None; 3];
        let retry = retain_pack_outcomes(
            vec![4_usize - 4, 1, 2],
            vec![
                Ok(outcome(21)),
                Err(Status::unavailable("lost response")),
                Ok(outcome(23)),
            ],
            &mut outcomes,
        )
        .unwrap();
        assert_eq!(retry, [1]);
        assert_eq!(outcomes[0].as_ref().unwrap().version, VersionId(21));
        assert!(outcomes[1].is_none());
        assert_eq!(outcomes[2].as_ref().unwrap().version, VersionId(23));
        assert!(
            retain_pack_outcomes(
                retry,
                vec![Ok(IndexArtifactOutcome {
                    version: VersionId(22),
                    replayed: true,
                })],
                &mut outcomes,
            )
            .unwrap()
            .is_empty()
        );
        assert_eq!(outcomes[1].as_ref().unwrap().version, VersionId(22));
    }

    #[test]
    fn permanent_item_failure_cannot_produce_complete_receipts() {
        let mut outcomes = vec![None; 2];
        assert!(
            retain_pack_outcomes(
                vec![0, 1],
                vec![Ok(outcome(31)), Err(Status::data_loss("bad receipt"))],
                &mut outcomes,
            )
            .is_err()
        );
        assert!(outcomes[0].is_some());
        assert!(outcomes[1].is_none());
    }
}
