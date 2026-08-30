//! Restart-safe, low-frequency reclamation of unpublished immutable artifacts.

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use keldra_consensus::NodeId;
use keldra_store::{
    DefinitionKind, IndexOrphanScrubDue, ObjectKey, ObjectRecordCursor, Store, VersionId,
};
use tonic::{Code, Status};

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_runtime::cache::IndexMergeScratchSpace;
use crate::index_runtime::catalog::CatalogDefinition;
use crate::index_runtime::coordination::load_definition_locator_object;
use crate::index_runtime::publication::{
    IndexArtifactDelete, IndexArtifactRouter, artifact_hash_from_path, current_path,
    is_manifest_artifact_path, rebuild_path,
};
use crate::index_runtime::publisher::{
    CommittedIndexView, IndexCommitPublisher, LoadedRebuildRoot,
};
use crate::index_runtime::scanner::ClusterIndexScanner;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::scratch::{
    RetainedObjectCollector, RetainedObjectProof, RetainedObjectRecord, RetainedObjectSort,
};
use super::{
    IndexRetentionBudget, RETAINED_ARTIFACT_CLASS, RETAINED_MANIFEST_CLASS,
    UNREACHABLE_ARTIFACT_SAFETY_MILLIS, now_unix_millis, prepare_pack,
};

const SCRUB_INTERVAL_MILLIS: u64 = 60 * 60 * 1_000;
const CURSOR_RETRY_MILLIS: u64 = 100;

#[derive(Clone)]
pub(super) struct IndexOrphanScrub {
    store: Store,
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    publisher: IndexCommitPublisher,
    scratch: IndexMergeScratchSpace,
    budget: IndexRetentionBudget,
    active_proof: Arc<tokio::sync::Mutex<Option<OrphanProofState>>>,
}

#[derive(Clone)]
struct OrphanAuthority {
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
    current_version: VersionId,
    pointer: crate::index_runtime::committed_view::IndexCurrentPointer,
    rebuild: Option<LoadedRebuildRoot>,
}

enum OrphanProofState {
    Collect(OrphanProofCollection),
    Sort(OrphanProofSort),
    Ready(OrphanProtectedProof),
}

struct OrphanProofCollection {
    authority: OrphanAuthority,
    collector: RetainedObjectCollector,
    cursor: OrphanCollectionCursor,
}

struct OrphanCollectionCursor {
    pending_manifests: VecDeque<crate::index_runtime::committed_view::CommitManifestReference>,
    rebuild_pending: bool,
}

enum OrphanCollectionAction {
    Manifest(crate::index_runtime::committed_view::CommitManifestReference),
    Rebuild,
    Sort,
}

struct OrphanProofSort {
    authority: OrphanAuthority,
    sort: RetainedObjectSort,
}

struct OrphanProtectedProof {
    authority: OrphanAuthority,
    proof: RetainedObjectProof,
}

impl IndexOrphanScrub {
    pub(super) fn new(
        store: Store,
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        publisher: IndexCommitPublisher,
        scratch: IndexMergeScratchSpace,
    ) -> Self {
        Self {
            store,
            scanner,
            reader,
            artifacts,
            publisher,
            scratch,
            budget: IndexRetentionBudget::default(),
            active_proof: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub(super) fn with_budget(mut self, budget: IndexRetentionBudget) -> Self {
        self.budget = budget;
        self
    }

    pub(super) fn schedule_if_absent(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        definition_object_version: u64,
    ) -> Result<(), Status> {
        let due = IndexOrphanScrubDue {
            tenant_id,
            bucket_id,
            index_id: definition.index_id,
            definition_path: definition_path(&definition.name)?,
            definition_object_version: VersionId(definition_object_version),
            due_at_unix_millis: now_unix_millis()?.saturating_add(SCRUB_INTERVAL_MILLIS),
            scan_placement_term: 0,
            scan_placement_index: 0,
            scan_node_id: 0,
            scan_cursor: None,
        };
        self.store
            .schedule_index_orphan_scrub_if_absent(&due)
            .map(|_| ())
            .map_err(orphan_due_status)
    }

    pub(super) fn oldest_due(&self) -> Result<Option<IndexOrphanScrubDue>, Status> {
        self.store
            .oldest_index_orphan_scrub_due()
            .map_err(orphan_due_status)
    }

    pub(super) async fn run_tick(&self) -> Result<u64, Status> {
        let Some(due) = self.oldest_due()? else {
            return Ok(0);
        };
        if due.due_at_unix_millis > now_unix_millis()? {
            return Ok(0);
        }
        if !self
            .store
            .index_orphan_scrub_due_matches(&due)
            .map_err(orphan_due_status)?
        {
            return Ok(0);
        }
        if !self
            .artifacts
            .is_local_builder(due.tenant_id, due.bucket_id, due.index_id)?
        {
            self.store
                .cancel_index_orphan_scrub(due.tenant_id, due.bucket_id, due.index_id)
                .map_err(orphan_due_status)?;
            return Ok(0);
        }
        let Some((definition, current, rebuild)) = self.load_authority(&due).await? else {
            return Ok(0);
        };
        if !self
            .advance_protected_proof(
                &definition,
                due.tenant_id,
                due.bucket_id,
                &current,
                rebuild.as_ref(),
            )
            .await?
        {
            self.replace_due(
                &due,
                retry_due(&due, now_unix_millis()?.saturating_add(CURSOR_RETRY_MILLIS)),
            )?;
            return Ok(0);
        }
        let scope = IndexHeadScanScope {
            tenant_id: due.tenant_id,
            bucket_id: due.bucket_id,
            index_id: due.index_id,
        };
        let mut scan = if due.scan_is_new() {
            self.scanner.begin(scope)?
        } else {
            let cursor = due
                .scan_cursor
                .as_deref()
                .map(ObjectRecordCursor::from_token)
                .transpose()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            match self.scanner.begin_at(
                scope,
                keldra_store::PlacementLogId {
                    term: due.scan_placement_term,
                    index: due.scan_placement_index,
                },
                NodeId(due.scan_node_id),
                cursor,
            ) {
                Ok(scan) => scan,
                Err(error) if error.code() == Code::Aborted => {
                    self.replace_due(&due, reset_due(&due, now_unix_millis()?))?;
                    return Ok(0);
                }
                Err(error) => return Err(error),
            }
        };
        let heads = scan.next_page().await?;
        let now = now_unix_millis()?;
        let mut observed = 0_u64;
        let mut observed_bytes = 0_u64;
        let mut oldest_age_millis = 0_u64;
        let mut removed = 0_u64;
        if let Some(heads) = heads {
            for head in heads {
                let version = &head.version;
                if version.deleted {
                    continue;
                }
                let blob = version.blob.as_ref().ok_or_else(|| {
                    Status::data_loss("live orphan candidate has no blob reference")
                })?;
                let protected = {
                    let mut active = self.active_proof.lock().await;
                    let Some(OrphanProofState::Ready(proof)) = active.as_mut() else {
                        return Err(Status::internal(
                            "orphan protected proof disappeared during scan",
                        ));
                    };
                    proof
                        .contains(&head.exact_path, version.id, blob.hash, definition.index_id)
                        .await?
                };
                if protected {
                    continue;
                }
                observed = observed.saturating_add(1);
                observed_bytes = observed_bytes.saturating_add(blob.length);
                oldest_age_millis =
                    oldest_age_millis.max(now.saturating_sub(version.committed_at_unix_millis));
                if now
                    < version
                        .committed_at_unix_millis
                        .saturating_add(UNREACHABLE_ARTIFACT_SAFETY_MILLIS)
                {
                    continue;
                }
                self.delete_if_still_orphan(
                    &due,
                    &definition,
                    &current,
                    rebuild.as_ref(),
                    &head.exact_path,
                    version.id,
                )
                .await?;
                removed = removed.saturating_add(1);
            }
        }
        tracing::debug!(
            index.id = due.index_id,
            tenant.id = due.tenant_id,
            bucket.id = due.bucket_id,
            gauge.keldra_index_orphan_objects = observed,
            gauge.keldra_index_orphan_bytes = observed_bytes,
            gauge.keldra_index_orphan_oldest_age_seconds = oldest_age_millis as f64 / 1_000.0,
            monotonic_counter.keldra_index_orphan_objects_reclaimed_total = removed,
            "bounded index orphan scrub page completed"
        );
        let replacement = match scan.checkpoint() {
            Some((fence, node, cursor)) => IndexOrphanScrubDue {
                due_at_unix_millis: now.saturating_add(CURSOR_RETRY_MILLIS),
                scan_placement_term: fence.term,
                scan_placement_index: fence.index,
                scan_node_id: node.0,
                scan_cursor: cursor.map(|cursor| cursor.as_token().to_owned()),
                ..due.clone()
            },
            None => {
                *self.active_proof.lock().await = None;
                reset_due(&due, now.saturating_add(SCRUB_INTERVAL_MILLIS))
            }
        };
        self.replace_due(&due, replacement)?;
        Ok(removed)
    }

    async fn load_authority(
        &self,
        due: &IndexOrphanScrubDue,
    ) -> Result<
        Option<(
            StoredIndexDefinition,
            CommittedIndexView,
            Option<LoadedRebuildRoot>,
        )>,
        Status,
    > {
        let Some(locator) = self
            .store
            .definition_locator(
                DefinitionKind::Index,
                due.tenant_id,
                due.bucket_id,
                &due.definition_path,
            )
            .map_err(|error| Status::unavailable(error.to_string()))?
        else {
            return Ok(None);
        };
        if locator.object_version != due.definition_object_version {
            self.store
                .cancel_index_orphan_scrub(due.tenant_id, due.bucket_id, due.index_id)
                .map_err(orphan_due_status)?;
            return Ok(None);
        }
        let Some(object) = load_definition_locator_object(&self.reader, &locator).await? else {
            return Err(Status::unavailable(
                "orphan scrub definition is not exact-readable",
            ));
        };
        let definition = StoredIndexDefinition::decode(&object.bytes)?;
        if definition.index_id != locator.definition_id
            || definition_path(&definition.name)? != due.definition_path
        {
            return Err(Status::data_loss(
                "orphan scrub definition identity is inconsistent",
            ));
        }
        let catalog = CatalogDefinition::new(
            due.tenant_id,
            due.bucket_id,
            locator.object_version.0,
            definition,
        )?;
        if catalog.physical_index_id() != due.index_id {
            return Err(Status::data_loss(
                "orphan scrub physical identity is inconsistent",
            ));
        }
        let definition = catalog.physical_stored();
        let current = self
            .publisher
            .load_current(&definition, due.tenant_id, due.bucket_id)
            .await?
            .ok_or_else(|| Status::unavailable("orphan scrub index has no committed view"))?;
        if current.manifest.definition_version != catalog.physical_definition_version() {
            return Err(Status::aborted(
                "orphan scrub current view belongs to another definition revision",
            ));
        }
        let rebuild = self
            .publisher
            .load_rebuild_root(&definition, due.tenant_id, due.bucket_id)
            .await?;
        Ok(Some((definition, current, rebuild)))
    }

    async fn advance_protected_proof(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: &CommittedIndexView,
        rebuild: Option<&LoadedRebuildRoot>,
    ) -> Result<bool, Status> {
        {
            let mut active = self.active_proof.lock().await;
            if active
                .as_ref()
                .is_some_and(|proof| proof.matches(tenant_id, bucket_id, current, rebuild))
            {
                if matches!(active.as_ref(), Some(OrphanProofState::Ready(_))) {
                    return Ok(true);
                }
            } else {
                *active = None;
            }
        }
        if self.active_proof.lock().await.is_none() {
            let authority = OrphanAuthority::new(tenant_id, bucket_id, current, rebuild);
            let pending_manifests = std::iter::once(&current.pointer.current)
                .chain(current.pointer.retained.iter())
                .chain(
                    current
                        .pointer
                        .releasing
                        .iter()
                        .map(|released| &released.manifest),
                )
                .cloned()
                .collect();
            let collector = RetainedObjectCollector::new(self.scratch.clone()).await?;
            *self.active_proof.lock().await =
                Some(OrphanProofState::Collect(OrphanProofCollection {
                    authority,
                    collector,
                    cursor: OrphanCollectionCursor {
                        pending_manifests,
                        rebuild_pending: rebuild.is_some(),
                    },
                }));
        }

        let state = self.active_proof.lock().await.take().ok_or_else(|| {
            Status::internal("orphan protected proof state disappeared before advancement")
        })?;
        let (state, ready) = match state {
            OrphanProofState::Collect(mut collection) => match collection.cursor.next_action() {
                OrphanCollectionAction::Manifest(reference) => {
                    let started = Instant::now();
                    let result = async {
                        let manifest = self
                            .load_manifest(definition, tenant_id, bucket_id, &reference)
                            .await?;
                        let mut records = vec![RetainedObjectRecord::new(
                            RETAINED_MANIFEST_CLASS,
                            reference.blob.hash,
                            reference.object_version.0,
                            reference.blob.length,
                            0,
                        )?];
                        append_manifest_pack_records(&manifest, &mut records)?;
                        collection.collector.append(records).await
                    }
                    .await;
                    if let Err(error) = result {
                        collection.cursor.pending_manifests.push_front(reference);
                        *self.active_proof.lock().await =
                            Some(OrphanProofState::Collect(collection));
                        return Err(error);
                    }
                    tracing::debug!(
                        index.id = collection.authority.index_id,
                        histogram.keldra_index_orphan_proof_manifest_seconds =
                            started.elapsed().as_secs_f64(),
                        "orphan protected manifest appended to bounded external proof",
                    );
                    (OrphanProofState::Collect(collection), false)
                }
                OrphanCollectionAction::Rebuild => {
                    let started = Instant::now();
                    let result = async {
                        if let Some(rebuild) = collection.authority.rebuild.as_ref() {
                            let mut records = Vec::new();
                            append_manifest_pack_records(&rebuild.root.candidate, &mut records)?;
                            collection.collector.append(records).await?;
                        }
                        Ok::<_, Status>(())
                    }
                    .await;
                    if let Err(error) = result {
                        collection.cursor.rebuild_pending = true;
                        *self.active_proof.lock().await =
                            Some(OrphanProofState::Collect(collection));
                        return Err(error);
                    }
                    tracing::debug!(
                        index.id = collection.authority.index_id,
                        histogram.keldra_index_orphan_proof_rebuild_seconds =
                            started.elapsed().as_secs_f64(),
                        "orphan rebuild packs appended to bounded external proof",
                    );
                    (OrphanProofState::Collect(collection), false)
                }
                OrphanCollectionAction::Sort => (
                    OrphanProofState::Sort(OrphanProofSort {
                        authority: collection.authority,
                        sort: collection.collector.into_sort(),
                    }),
                    false,
                ),
            },
            OrphanProofState::Sort(mut sorting) => {
                let advanced = sorting
                    .sort
                    .advance(Instant::now() + self.budget.max_time)
                    .await;
                match advanced {
                    Ok(Some(proof)) => (
                        OrphanProofState::Ready(OrphanProtectedProof {
                            authority: sorting.authority,
                            proof,
                        }),
                        true,
                    ),
                    Ok(None) => (OrphanProofState::Sort(sorting), false),
                    Err(error) => {
                        *self.active_proof.lock().await = Some(OrphanProofState::Sort(sorting));
                        return Err(error);
                    }
                }
            }
            ready @ OrphanProofState::Ready(_) => (ready, true),
        };
        *self.active_proof.lock().await = Some(state);
        Ok(ready)
    }

    async fn load_manifest(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        reference: &crate::index_runtime::committed_view::CommitManifestReference,
    ) -> Result<crate::index_runtime::committed_view::IndexCommitManifest, Status> {
        let key = ObjectKey::new(&definition.tenant, &definition.bucket, &reference.path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, Some(reference.object_version))
            .await?
        else {
            return Err(Status::data_loss("protected index manifest is absent"));
        };
        if opened.version.deleted || opened.version.blob.as_ref() != Some(&reference.blob) {
            return Err(Status::data_loss(
                "protected index manifest differs from its exact reference",
            ));
        }
        let mut bytes = Vec::new();
        opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("protected index manifest has no payload"))?
            .take(reference.blob.length.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read protected index manifest: {error}")))?;
        if bytes.len() as u64 != reference.blob.length {
            return Err(Status::data_loss(
                "protected index manifest length is inconsistent",
            ));
        }
        crate::index_runtime::committed_view::IndexCommitManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete_if_still_orphan(
        &self,
        due: &IndexOrphanScrubDue,
        definition: &StoredIndexDefinition,
        expected_current: &CommittedIndexView,
        expected_rebuild: Option<&LoadedRebuildRoot>,
        path: &str,
        version: VersionId,
    ) -> Result<(), Status> {
        let current_guard = self
            .artifacts
            .acquire_current_mutation(due.index_id)
            .await?;
        if !self
            .store
            .index_orphan_scrub_due_matches(due)
            .map_err(orphan_due_status)?
        {
            return Err(Status::aborted("orphan scrub schedule changed"));
        }
        let observed_current = self
            .publisher
            .load_current(definition, due.tenant_id, due.bucket_id)
            .await?
            .ok_or_else(|| Status::aborted("orphan scrub current pointer disappeared"))?;
        if observed_current.current_object_version != expected_current.current_object_version
            || observed_current.pointer != expected_current.pointer
        {
            return Err(Status::aborted(
                "orphan scrub current pointer changed before deletion",
            ));
        }
        let observed_rebuild = self
            .publisher
            .load_rebuild_root(definition, due.tenant_id, due.bucket_id)
            .await?;
        if !same_rebuild(expected_rebuild, observed_rebuild.as_ref()) {
            return Err(Status::aborted(
                "orphan scrub rebuild root changed before deletion",
            ));
        }
        self.artifacts
            .delete_while_current_mutation_held(
                IndexArtifactDelete {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id: due.tenant_id,
                    bucket_id: due.bucket_id,
                    index_id: due.index_id,
                    exact_path: path.to_owned(),
                    expected_version: version,
                    command_id: super::delete_command(due.index_id, version, "orphan", path),
                    definition_intent: None,
                },
                &current_guard,
            )
            .await?;
        Ok(())
    }

    fn replace_due(
        &self,
        expected: &IndexOrphanScrubDue,
        replacement: IndexOrphanScrubDue,
    ) -> Result<(), Status> {
        if !self
            .store
            .replace_index_orphan_scrub_due(expected, &replacement)
            .map_err(orphan_due_status)?
        {
            return Err(Status::aborted(
                "orphan scrub schedule changed before checkpoint",
            ));
        }
        Ok(())
    }
}

impl OrphanProofState {
    fn matches(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        current: &CommittedIndexView,
        rebuild: Option<&LoadedRebuildRoot>,
    ) -> bool {
        self.authority()
            .matches(tenant_id, bucket_id, current, rebuild)
    }

    fn authority(&self) -> &OrphanAuthority {
        match self {
            Self::Collect(state) => &state.authority,
            Self::Sort(state) => &state.authority,
            Self::Ready(state) => &state.authority,
        }
    }
}

impl OrphanCollectionCursor {
    fn next_action(&mut self) -> OrphanCollectionAction {
        if let Some(reference) = self.pending_manifests.pop_front() {
            OrphanCollectionAction::Manifest(reference)
        } else if self.rebuild_pending {
            self.rebuild_pending = false;
            OrphanCollectionAction::Rebuild
        } else {
            OrphanCollectionAction::Sort
        }
    }
}

impl OrphanProtectedProof {
    async fn contains(
        &mut self,
        path: &str,
        version: VersionId,
        blob_hash: [u8; 32],
        index_id: u64,
    ) -> Result<bool, Status> {
        if path == current_path(index_id) && version == self.authority.current_version {
            return Ok(true);
        }
        if path == rebuild_path(index_id)
            && self
                .authority
                .rebuild
                .as_ref()
                .is_some_and(|rebuild| rebuild.object_version == version)
        {
            return Ok(true);
        }
        let class = if is_manifest_artifact_path(index_id, path) {
            RETAINED_MANIFEST_CLASS
        } else if artifact_hash_from_path(index_id, path).is_some() {
            RETAINED_ARTIFACT_CLASS
        } else {
            return Ok(false);
        };
        self.proof
            .lookup(class, blob_hash, version.0)
            .await
            .map(|record| record.is_some())
    }
}

impl OrphanAuthority {
    fn new(
        tenant_id: u64,
        bucket_id: u64,
        current: &CommittedIndexView,
        rebuild: Option<&LoadedRebuildRoot>,
    ) -> Self {
        Self {
            tenant_id,
            bucket_id,
            index_id: current.manifest.index_id,
            current_version: current.current_object_version,
            pointer: current.pointer.clone(),
            rebuild: rebuild.cloned(),
        }
    }

    fn matches(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        current: &CommittedIndexView,
        rebuild: Option<&LoadedRebuildRoot>,
    ) -> bool {
        authority_matches(
            self.tenant_id,
            self.bucket_id,
            self.index_id,
            self.current_version,
            &self.pointer,
            self.rebuild.as_ref(),
            tenant_id,
            bucket_id,
            current.manifest.index_id,
            current.current_object_version,
            &current.pointer,
            rebuild,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn authority_matches(
    expected_tenant_id: u64,
    expected_bucket_id: u64,
    expected_index_id: u64,
    expected_current_version: VersionId,
    expected_pointer: &crate::index_runtime::committed_view::IndexCurrentPointer,
    expected_rebuild: Option<&LoadedRebuildRoot>,
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
    current_version: VersionId,
    pointer: &crate::index_runtime::committed_view::IndexCurrentPointer,
    rebuild: Option<&LoadedRebuildRoot>,
) -> bool {
    expected_tenant_id == tenant_id
        && expected_bucket_id == bucket_id
        && expected_index_id == index_id
        && expected_current_version == current_version
        && expected_pointer == pointer
        && same_rebuild(expected_rebuild, rebuild)
}

fn append_manifest_pack_records(
    manifest: &crate::index_runtime::committed_view::IndexCommitManifest,
    records: &mut Vec<RetainedObjectRecord>,
) -> Result<(), Status> {
    for pack in
        manifest
            .segments
            .iter()
            .flat_map(|segment| segment.packs.iter())
            .chain(manifest.locator_roots.iter().flat_map(
                |locator| match &locator.pack_ownership {
                    crate::index_runtime::committed_view::LocatorPackOwnership::Segment => {
                        [].as_slice()
                    }
                    crate::index_runtime::committed_view::LocatorPackOwnership::Standalone(
                        packs,
                    ) => packs.as_slice(),
                },
            ))
    {
        prepare_pack(manifest.index_id, 0, pack, records)?;
    }
    Ok(())
}

fn same_rebuild(
    expected: Option<&LoadedRebuildRoot>,
    observed: Option<&LoadedRebuildRoot>,
) -> bool {
    match (expected, observed) {
        (None, None) => true,
        (Some(expected), Some(observed)) => {
            expected.object_version == observed.object_version && expected.root == observed.root
        }
        _ => false,
    }
}

fn reset_due(due: &IndexOrphanScrubDue, due_at_unix_millis: u64) -> IndexOrphanScrubDue {
    IndexOrphanScrubDue {
        due_at_unix_millis,
        scan_placement_term: 0,
        scan_placement_index: 0,
        scan_node_id: 0,
        scan_cursor: None,
        ..due.clone()
    }
}

fn retry_due(due: &IndexOrphanScrubDue, due_at_unix_millis: u64) -> IndexOrphanScrubDue {
    IndexOrphanScrubDue {
        due_at_unix_millis,
        ..due.clone()
    }
}

fn orphan_due_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use keldra_store::BlobRef;

    use super::*;
    use crate::index_runtime::committed_view::{CommitManifestReference, IndexCurrentPointer};
    use crate::index_runtime::publication::manifest_path;

    fn reference(revision: u64) -> CommitManifestReference {
        let hash = *blake3::hash(&revision.to_be_bytes()).as_bytes();
        CommitManifestReference {
            revision,
            definition_version: 1,
            schema_fingerprint: [1; 32],
            path: manifest_path(9, hash),
            blob: BlobRef { hash, length: 256 },
            object_version: VersionId(revision),
            published_at_unix_millis: 1,
            retained_bytes: 256,
        }
    }

    #[test]
    fn protected_proof_collection_requires_one_quantum_per_authority_root() {
        let mut cursor = OrphanCollectionCursor {
            pending_manifests: VecDeque::from([reference(3), reference(2)]),
            rebuild_pending: true,
        };
        assert!(matches!(
            cursor.next_action(),
            OrphanCollectionAction::Manifest(reference) if reference.revision == 3
        ));
        assert!(matches!(
            cursor.next_action(),
            OrphanCollectionAction::Manifest(reference) if reference.revision == 2
        ));
        assert!(matches!(
            cursor.next_action(),
            OrphanCollectionAction::Rebuild
        ));
        assert!(matches!(cursor.next_action(), OrphanCollectionAction::Sort));
    }

    #[test]
    fn authority_change_invalidates_every_proof_phase() {
        let pointer =
            IndexCurrentPointer::new(9, reference(3), vec![reference(2)], Vec::new()).unwrap();
        assert!(authority_matches(
            1,
            2,
            9,
            VersionId(10),
            &pointer,
            None,
            1,
            2,
            9,
            VersionId(10),
            &pointer,
            None,
        ));
        assert!(!authority_matches(
            1,
            2,
            9,
            VersionId(10),
            &pointer,
            None,
            1,
            2,
            9,
            VersionId(11),
            &pointer,
            None,
        ));
        let newer =
            IndexCurrentPointer::new(9, reference(4), vec![reference(3)], Vec::new()).unwrap();
        assert!(!authority_matches(
            1,
            2,
            9,
            VersionId(10),
            &pointer,
            None,
            1,
            2,
            9,
            VersionId(10),
            &newer,
            None,
        ));
    }

    #[test]
    fn proof_retry_preserves_scan_progress_but_placement_abort_resets_it() {
        let due = IndexOrphanScrubDue {
            tenant_id: 1,
            bucket_id: 2,
            index_id: 9,
            definition_path: "_keldra/indexes/search".into(),
            definition_object_version: VersionId(4),
            due_at_unix_millis: 100,
            scan_placement_term: 7,
            scan_placement_index: 8,
            scan_node_id: 3,
            scan_cursor: Some("cursor".into()),
        };
        let retry = retry_due(&due, 200);
        assert_eq!(retry.scan_placement_term, 7);
        assert_eq!(retry.scan_placement_index, 8);
        assert_eq!(retry.scan_node_id, 3);
        assert_eq!(retry.scan_cursor.as_deref(), Some("cursor"));

        let reset = reset_due(&due, 200);
        assert!(reset.scan_is_new());
    }
}
