//! Snapshot-bound initial backfill for one format-v6 source partition.
//!
//! A backfill is deliberately a pull cursor rather than a collected result.
//! The source snapshot owns bounded RPC frames and this cursor returns one
//! prepared selection at a time, so a definition created after a large corpus
//! does not materialize that corpus in the producer.

use std::collections::VecDeque;

use keldra_index::v6::{
    IndexingMemoryCredits, IndexingMemoryPermit, IndexingMemoryStage, ProjectionPartitionIdentity,
};
use keldra_store::SourceId;
use tonic::Status;

use crate::cluster_peer::IndexSourceSnapshotHead;

use super::catalog::PhysicalCatalogRecipe;
use super::scanner::{ClusterIndexScanner, ClusterIndexSourceSnapshot};
use super::source::{IndexBuildObject, IndexSourceMutation};
use super::v6_extractor::{SelectedV6Source, V6ProjectionExtractor, matching_recipes};

/// Pull cursor over the current objects belonging to one exact source
/// incarnation at one captured journal tail.
pub(crate) struct V6PartitionBaseline {
    snapshot: ClusterIndexSourceSnapshot,
    recipe: PhysicalCatalogRecipe,
    source: SourceId,
    captured_tail: u64,
    captured_next_offset: u64,
    next_baseline_offset: u64,
    pending: VecDeque<IndexSourceSnapshotHead>,
    ended: bool,
}

/// One definition-neutral selection from the bounded baseline cursor.
///
/// `baseline_offset` is dense snapshot order, not the historical journal
/// offset. It lets the ordered producer feed bounded batches into its
/// accumulator. `source_journal_offset` is retained as source-lineage evidence.
pub(crate) struct V6BaselineSelected {
    pub(crate) baseline_offset: u64,
    pub(crate) source_journal_offset: u64,
    pub(crate) source_bytes: u64,
    pub(crate) selected: SelectedV6Source,
    _memory: IndexingMemoryPermit,
}

/// Open a partition-exact baseline at the source snapshot's captured tail.
pub(crate) async fn open_partition_baseline(
    scanner: &ClusterIndexScanner,
    recipe: &PhysicalCatalogRecipe,
    source: SourceId,
    partition: ProjectionPartitionIdentity,
    maximum_frame_bytes: u64,
) -> Result<V6PartitionBaseline, Status> {
    require_partition(recipe, source, partition)?;
    let snapshot = scanner
        .begin_source_snapshot(
            recipe.family.tenant_id,
            recipe.family.bucket_id,
            recipe.path_prefix.clone(),
            None,
            maximum_frame_bytes,
        )
        .await?;
    if snapshot.placement_fence().term != partition.placement_term
        || snapshot.placement_fence().index != partition.placement_index
    {
        return Err(Status::unavailable(
            "v6 baseline snapshot belongs to another placement fence",
        ));
    }
    let checkpoint = snapshot
        .checkpoints()
        .iter()
        .find(|checkpoint| checkpoint.source == source)
        .ok_or_else(|| Status::unavailable("v6 baseline source is not ACTIVE"))?;
    let captured_tail = checkpoint.captured_tail;
    let captured_next_offset = captured_tail
        .checked_add(1)
        .ok_or_else(|| Status::data_loss("v6 baseline source tail overflow"))?;
    Ok(V6PartitionBaseline {
        snapshot,
        recipe: recipe.clone(),
        source,
        captured_tail,
        captured_next_offset,
        next_baseline_offset: 0,
        pending: VecDeque::new(),
        ended: false,
    })
}

impl V6PartitionBaseline {
    /// First journal offset not represented by this point-in-time baseline.
    pub(crate) const fn captured_next_offset(&self) -> u64 {
        self.captured_next_offset
    }

    /// Return one source-exact selected object while holding its explicit
    /// replay-input reservation. Callers should drop the returned value after
    /// transferring it into their prepared-row/query budgets.
    pub(crate) async fn next_selected(
        &mut self,
        extractor: &V6ProjectionExtractor,
        credits: &IndexingMemoryCredits,
        maximum_selected_bytes: usize,
    ) -> Result<Option<V6BaselineSelected>, Status> {
        if maximum_selected_bytes == 0 {
            return Err(Status::invalid_argument(
                "v6 baseline selected-object budget must be positive",
            ));
        }
        loop {
            if let Some(head) = self.pending.front().cloned() {
                let construction_memory = credits
                    .acquire(IndexingMemoryStage::ReplayInput, maximum_selected_bytes)
                    .map_err(|_| {
                        Status::resource_exhausted("v6 baseline selection memory unavailable")
                    })?;
                let Some((object, source_journal_offset)) = baseline_object(
                    head,
                    self.source,
                    &self.recipe.path_prefix,
                    self.recipe.content_type.as_deref(),
                    self.captured_tail,
                )?
                else {
                    self.pending.pop_front();
                    continue;
                };
                let source_bytes = object.content_length;
                let source = IndexSourceMutation::Upsert(object);
                let recipes = matching_recipes(
                    std::slice::from_ref(&self.recipe),
                    self.recipe.family.tenant_id,
                    self.recipe.family.bucket_id,
                    match &source {
                        IndexSourceMutation::Upsert(object) => &object.path,
                        IndexSourceMutation::Remove(_) => unreachable!("baseline is live-only"),
                    },
                    match &source {
                        IndexSourceMutation::Upsert(object) => object.content_type.as_deref(),
                        IndexSourceMutation::Remove(_) => unreachable!("baseline is live-only"),
                    },
                );
                if recipes.is_empty() {
                    self.pending.pop_front();
                    continue;
                }
                let selected = extractor
                    .select(
                        self.recipe.family.tenant_id,
                        self.recipe.family.bucket_id,
                        source,
                        &recipes,
                    )
                    .await?;
                let resident_bytes = selected_resident_bytes(&selected)?;
                if resident_bytes > maximum_selected_bytes {
                    return Err(Status::resource_exhausted(format!(
                        "v6 baseline selection requires {resident_bytes} bytes but its bound is {maximum_selected_bytes}"
                    )));
                }
                drop(construction_memory);
                let memory = credits
                    .acquire(IndexingMemoryStage::ReplayInput, resident_bytes.max(1))
                    .map_err(|_| {
                        Status::resource_exhausted(
                            "v6 baseline retained selection memory unavailable",
                        )
                    })?;
                let baseline_offset = self.next_baseline_offset;
                self.next_baseline_offset = self
                    .next_baseline_offset
                    .checked_add(1)
                    .ok_or_else(|| Status::data_loss("v6 baseline object count overflow"))?;
                if self.next_baseline_offset >= self.captured_next_offset {
                    return Err(Status::data_loss(
                        "v6 baseline contains more current source heads than journal positions",
                    ));
                }
                self.pending.pop_front();
                return Ok(Some(V6BaselineSelected {
                    baseline_offset,
                    source_journal_offset,
                    source_bytes,
                    selected,
                    _memory: memory,
                }));
            }
            if self.ended {
                return Ok(None);
            }
            match self.snapshot.next_frame().await? {
                Some(frame) => self.pending.extend(frame),
                None => self.ended = true,
            }
        }
    }

    /// Pull a bounded group suitable for one accumulator application. Each
    /// item retains only its measured memory charge; `max_items` bounds CPU and
    /// accumulator clone cost independently of source snapshot frame size.
    pub(crate) async fn next_selected_batch(
        &mut self,
        extractor: &V6ProjectionExtractor,
        credits: &IndexingMemoryCredits,
        maximum_selected_bytes: usize,
        max_items: usize,
    ) -> Result<Vec<V6BaselineSelected>, Status> {
        if max_items == 0 {
            return Err(Status::invalid_argument(
                "v6 baseline selected batch must allow at least one item",
            ));
        }
        let mut selected = Vec::with_capacity(max_items.min(4_096));
        while selected.len() < max_items {
            match self
                .next_selected(extractor, credits, maximum_selected_bytes)
                .await
            {
                Ok(Some(item)) => selected.push(item),
                Ok(None) => break,
                Err(error)
                    if !selected.is_empty() && error.code() == tonic::Code::ResourceExhausted =>
                {
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(selected)
    }
}

fn require_partition(
    recipe: &PhysicalCatalogRecipe,
    source: SourceId,
    partition: ProjectionPartitionIdentity,
) -> Result<(), Status> {
    partition
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if partition.family_id != recipe.family.family_id
        || partition.source_node != u64::from(source.node_id)
        || partition.source_epoch != source.source_epoch
    {
        return Err(Status::data_loss(
            "v6 baseline recipe, source, and partition disagree",
        ));
    }
    Ok(())
}

fn baseline_object(
    head: IndexSourceSnapshotHead,
    source: SourceId,
    path_prefix: &str,
    content_type: Option<&str>,
    captured_tail: u64,
) -> Result<Option<(IndexBuildObject, u64)>, Status> {
    if head.head.deleted
        || contains_reserved_segment(&head.exact_path)
        || !crate::index_service::path_matches_prefix(&head.exact_path, path_prefix)
        || content_type
            .is_some_and(|expected| head.version.content_type.as_deref() != Some(expected))
    {
        return Ok(None);
    }
    let Some(stamp) = head.head.mutation_stamp else {
        // A pre-journal 0.5.0 head has no source incarnation and therefore
        // cannot be attributed to an exact v6 source partition.
        return Ok(None);
    };
    if stamp.source_id != source {
        return Ok(None);
    }
    if stamp.source_journal_position > captured_tail {
        return Err(Status::data_loss(
            "v6 baseline head is newer than its captured source tail",
        ));
    }
    let blob = head
        .version
        .blob
        .ok_or_else(|| Status::data_loss("live v6 baseline source blob is absent"))?;
    Ok(Some((
        IndexBuildObject {
            path: head.exact_path,
            version: head.version.id.0,
            content_type: head.version.content_type,
            content_hash: blob.hash,
            content_length: blob.length,
            committed_at_unix_millis: head.version.committed_at_unix_millis,
        },
        stamp.source_journal_position,
    )))
}

fn selected_resident_bytes(selected: &SelectedV6Source) -> Result<usize, Status> {
    let source = match &selected.source {
        IndexSourceMutation::Upsert(object) => std::mem::size_of::<IndexBuildObject>()
            .checked_add(object.path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(
                    object
                        .content_type
                        .as_ref()
                        .map_or(0, |content_type| content_type.capacity()),
                )
            }),
        IndexSourceMutation::Remove(identity) => {
            std::mem::size_of_val(identity).checked_add(identity.path.capacity())
        }
    }
    .ok_or_else(|| Status::resource_exhausted("v6 baseline selection size overflow"))?;
    selected.selected.as_ref().map_or(Ok(source), |projection| {
        source
            .checked_add(projection.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("v6 baseline selection size overflow"))
    })
}

fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_keldra")
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use keldra_store::{BlobRef, Head, MutationStamp, PlacementLogId, Version, VersionId};

    use super::*;

    fn source(byte: u8) -> SourceId {
        SourceId {
            node_id: u16::from(byte),
            source_epoch: [byte; 32],
        }
    }

    fn head(path: &str, owner: SourceId, offset: u64) -> IndexSourceSnapshotHead {
        IndexSourceSnapshotHead {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: path.into(),
            head: Head {
                version: VersionId(7),
                deleted: false,
                mutation_stamp: Some(MutationStamp {
                    format: keldra_store::MUTATION_STAMP_FORMAT,
                    predecessor_version: None,
                    program_commit_cursor: None,
                    mutation_fingerprint: [3; 32],
                    active_placement_log_id: PlacementLogId { term: 4, index: 5 },
                    serving_fence_term: 4,
                    source_id: owner,
                    source_journal_position: offset,
                }),
            },
            version: Version {
                id: VersionId(7),
                blob: Some(BlobRef {
                    hash: [8; 32],
                    length: 19,
                }),
                content_type: Some("application/json".into()),
                deleted: false,
                committed_at_unix_millis: 9,
                protected_link_descriptor: false,
            },
            alias_registry: None,
        }
    }

    #[test]
    fn baseline_is_exact_to_source_scope_and_captured_tail() {
        let expected = source(1);
        let selected = baseline_object(
            head("docs/a.json", expected, 11),
            expected,
            "docs/",
            Some("application/json"),
            11,
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.0.path, "docs/a.json");
        assert_eq!(selected.0.version, 7);
        assert_eq!(selected.1, 11);

        assert!(
            baseline_object(
                head("docs/a.json", source(2), 10),
                expected,
                "docs/",
                Some("application/json"),
                11,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            baseline_object(
                head("docs/a.json", expected, 12),
                expected,
                "docs/",
                Some("application/json"),
                11,
            )
            .unwrap_err()
            .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn baseline_excludes_reserved_and_nonmatching_objects() {
        let expected = source(1);
        for path in ["_keldra/indexes/x", "docs/_keldra/x", "other/a.json"] {
            assert!(
                baseline_object(
                    head(path, expected, 1),
                    expected,
                    "docs/",
                    Some("application/json"),
                    1,
                )
                .unwrap()
                .is_none(),
                "{path}"
            );
        }
        let mut wrong_type = head("docs/a.json", expected, 1);
        wrong_type.version.content_type = Some("text/plain".into());
        assert!(
            baseline_object(wrong_type, expected, "docs/", Some("application/json"), 1,)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unstamped_legacy_head_is_not_misattributed() {
        let expected = source(1);
        let mut legacy = head("docs/a.json", expected, 1);
        legacy.head.mutation_stamp = None;
        assert!(
            baseline_object(legacy, expected, "docs/", Some("application/json"), 1,)
                .unwrap()
                .is_none()
        );
    }
}
