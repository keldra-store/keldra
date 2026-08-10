//! Bounded generation retention over ordinary format-2 index objects.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_store::{ObjectKey, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::StoredIndexDefinition;

use super::generation::{IndexGenerationManifest, ManifestReference};
use super::publication::{
    IndexArtifactDelete, IndexArtifactRouter, current_path, is_manifest_artifact_path,
    run_hash_from_artifact_path,
};
use super::publisher::PublishedGeneration;
use super::scanner::ClusterIndexScanner;

const UNREACHABLE_ARTIFACT_SAFETY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
const PUBLIC_REQUEST_SAFETY_MILLIS: u64 = 30 * 1_000;

#[derive(Clone)]
pub(crate) struct IndexGenerationRetention {
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    config: IndexRuntimeConfig,
}

impl IndexGenerationRetention {
    pub(crate) fn new(
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        config: IndexRuntimeConfig,
    ) -> Self {
        Self {
            scanner,
            reader,
            artifacts,
            config,
        }
    }

    /// Keep the newest predecessor prefix admitted by all configured bounds.
    /// Once any bound is reached, every older predecessor is obsolete. The
    /// current generation is unconditional and can never be selected.
    pub(crate) async fn collect(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: &PublishedGeneration,
    ) -> Result<u64, Status> {
        require_current_identity(definition, current)?;
        let now = now_unix_millis()?;
        let mut retained = RetainedArtifacts::default();
        retained.insert(&current.pointer.manifest_path, &current.manifest);
        let mut retained_count = 1_u32;
        let mut retained_bytes = current.manifest.authoritative_bytes;
        let mut obsolete = false;
        let mut deleted = 0_u64;
        let mut successor_published_at = current.pointer.published_at_unix_millis;
        let mut previous = current.manifest.previous.clone();
        let mut expected_below = current.manifest.generation;

        while let Some(reference) = previous {
            validate_predecessor(&reference, expected_below, definition.index_id)?;
            let manifest = match self
                .load_manifest(definition, tenant_id, bucket_id, &reference)
                .await?
            {
                LoadedPredecessor::Present(manifest) => manifest,
                LoadedPredecessor::PreviouslyPruned => break,
            };
            let within_bounds = !obsolete
                && retain_predecessor(
                    retained_count,
                    retained_bytes,
                    &reference,
                    &manifest,
                    now,
                    self.config,
                );
            if within_bounds {
                retained.insert(&reference.path, &manifest);
                retained_count = retained_count.saturating_add(1);
                retained_bytes = retained_bytes
                    .checked_add(manifest.authoritative_bytes)
                    .ok_or_else(|| {
                        Status::resource_exhausted("retained index generation bytes overflow")
                    })?;
            } else {
                obsolete = true;
                // A request that pinned this generation immediately before its
                // successor published still has the full public deadline to
                // finish. Unknown/unreachable candidates use the longer 24h
                // safety in the sweep below.
                if now.saturating_sub(successor_published_at) >= PUBLIC_REQUEST_SAFETY_MILLIS {
                    deleted = deleted.saturating_add(
                        self.delete_obsolete_generation(
                            definition,
                            tenant_id,
                            bucket_id,
                            &reference,
                            &manifest,
                            &retained.run_hashes,
                        )
                        .await?,
                    );
                }
            }
            successor_published_at = reference.published_at_unix_millis;
            expected_below = manifest.generation;
            previous = manifest.previous.clone();
        }

        deleted = deleted.saturating_add(
            self.sweep_unreachable(definition, tenant_id, bucket_id, &retained, now)
                .await?,
        );
        Ok(deleted)
    }

    async fn load_manifest(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        reference: &ManifestReference,
    ) -> Result<LoadedPredecessor, Status> {
        let key = ObjectKey::new(&definition.tenant, &definition.bucket, &reference.path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, Some(reference.object_version))
            .await?
        else {
            let current = self.reader.head_stable(&key, tenant_id, bucket_id).await?;
            return classify_absent_predecessor(reference.object_version, current.as_ref());
        };
        if opened.version.id != reference.object_version
            || opened.version.deleted
            || opened.version.blob.as_ref() != Some(&reference.blob)
        {
            return Err(Status::data_loss(
                "index predecessor object differs from its manifest reference",
            ));
        }
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("live index predecessor manifest has no payload"))?;
        let mut bytes = Vec::new();
        payload.read_to_end(&mut bytes).map_err(|error| {
            Status::internal(format!("read index predecessor manifest: {error}"))
        })?;
        let manifest = IndexGenerationManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if manifest.index_id != definition.index_id
            || manifest.generation != reference.generation
            || manifest.definition_version != reference.definition_version
        {
            return Err(Status::data_loss(
                "index predecessor manifest identity differs from its reference",
            ));
        }
        Ok(LoadedPredecessor::Present(manifest))
    }

    async fn delete_obsolete_generation(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        reference: &ManifestReference,
        manifest: &IndexGenerationManifest,
        retained_runs: &BTreeSet<[u8; 32]>,
    ) -> Result<u64, Status> {
        let mut deleted = 0_u64;
        for run in &manifest.runs {
            if !retained_runs.contains(&run.root_blob.hash) {
                deleted = deleted.saturating_add(
                    self.delete_run(definition, tenant_id, bucket_id, run.root_blob.hash)
                        .await?,
                );
            }
        }
        self.delete_exact(
            definition,
            tenant_id,
            bucket_id,
            &reference.path,
            reference.object_version,
            "manifest",
        )
        .await?;
        Ok(deleted.saturating_add(1))
    }

    async fn delete_run(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        run_hash: [u8; 32],
    ) -> Result<u64, Status> {
        let mut scan = self.scanner.begin(IndexHeadScanScope::Run {
            tenant_id,
            bucket_id,
            index_id: definition.index_id,
            run_hash,
        })?;
        let mut deleted = 0_u64;
        while let Some(heads) = scan.next_page().await? {
            for head in heads {
                if head.head.deleted || head.version.deleted {
                    continue;
                }
                self.delete_exact(
                    definition,
                    tenant_id,
                    bucket_id,
                    &head.exact_path,
                    head.version.id,
                    "run",
                )
                .await?;
                deleted = deleted.saturating_add(1);
            }
        }
        Ok(deleted)
    }

    async fn sweep_unreachable(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        retained: &RetainedArtifacts,
        now: u64,
    ) -> Result<u64, Status> {
        let mut scan = self.scanner.begin(IndexHeadScanScope::Generation {
            tenant_id,
            bucket_id,
            index_id: definition.index_id,
        })?;
        let mut deleted = 0_u64;
        let current = current_path(definition.index_id);
        while let Some(heads) = scan.next_page().await? {
            for head in heads {
                if head.exact_path == current {
                    for version in head.versions.iter().filter(|version| {
                        version.id != head.head.version
                            && !version.deleted
                            && version.blob.is_some()
                    }) {
                        self.delete_exact(
                            definition,
                            tenant_id,
                            bucket_id,
                            &head.exact_path,
                            version.id,
                            "current",
                        )
                        .await?;
                        deleted = deleted.saturating_add(1);
                    }
                    continue;
                }
                if head.head.deleted || head.version.deleted {
                    continue;
                }
                let age = now.saturating_sub(head.version.committed_at_unix_millis);
                if age < UNREACHABLE_ARTIFACT_SAFETY_MILLIS {
                    continue;
                }
                let retained_path =
                    if is_manifest_artifact_path(definition.index_id, &head.exact_path) {
                        retained.manifest_paths.contains(&head.exact_path)
                    } else if let Some(run_hash) =
                        run_hash_from_artifact_path(definition.index_id, &head.exact_path)
                    {
                        retained.run_hashes.contains(&run_hash)
                    } else {
                        true
                    };
                if retained_path {
                    continue;
                }
                self.delete_exact(
                    definition,
                    tenant_id,
                    bucket_id,
                    &head.exact_path,
                    head.version.id,
                    "unreachable",
                )
                .await?;
                deleted = deleted.saturating_add(1);
            }
        }
        Ok(deleted)
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete_exact(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        version: VersionId,
        class: &str,
    ) -> Result<(), Status> {
        self.artifacts
            .delete(IndexArtifactDelete {
                storage_tenant: definition.tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
                exact_path: path.to_owned(),
                expected_version: version,
                command_id: delete_command(definition.index_id, version, class, path),
            })
            .await?;
        Ok(())
    }
}

enum LoadedPredecessor {
    Present(IndexGenerationManifest),
    PreviouslyPruned,
}

fn classify_absent_predecessor(
    referenced_version: VersionId,
    current: Option<&anvil_store::Version>,
) -> Result<LoadedPredecessor, Status> {
    // The caller has already validated the content-addressed v2 manifest path
    // and proved that its exact referenced descriptor is absent. Public
    // object mutations cannot address this reserved path, so a newer,
    // payload-free current tombstone can only be the durable result of the
    // internal artifact retention delete. No mutation stamp is required in
    // the one-node fast path, where ordinary local heads deliberately omit it.
    match current {
        Some(version)
            if version.id > referenced_version && version.deleted && version.blob.is_none() =>
        {
            Ok(LoadedPredecessor::PreviouslyPruned)
        }
        Some(_) => Err(Status::data_loss(
            "index predecessor version is absent while its object path remains live",
        )),
        None => Err(Status::data_loss(
            "index predecessor version disappeared without a retention tombstone",
        )),
    }
}

#[derive(Default)]
struct RetainedArtifacts {
    manifest_paths: BTreeSet<String>,
    run_hashes: BTreeSet<[u8; 32]>,
}

impl RetainedArtifacts {
    fn insert(&mut self, path: &str, manifest: &IndexGenerationManifest) {
        self.manifest_paths.insert(path.to_owned());
        self.run_hashes
            .extend(manifest.runs.iter().map(|run| run.root_blob.hash));
    }
}

fn require_current_identity(
    definition: &StoredIndexDefinition,
    current: &PublishedGeneration,
) -> Result<(), Status> {
    if current.pointer.index_id != definition.index_id
        || current.manifest.index_id != definition.index_id
        || current.pointer.generation != current.manifest.generation
        || current.pointer.definition_version != current.manifest.definition_version
        || current.pointer.manifest_path
            != super::publication::manifest_path(
                definition.index_id,
                current.pointer.manifest_blob.hash,
            )
    {
        return Err(Status::data_loss(
            "current index pointer and manifest identity differ during retention",
        ));
    }
    Ok(())
}

fn validate_predecessor(
    reference: &ManifestReference,
    expected_below: u64,
    index_id: u64,
) -> Result<(), Status> {
    if reference.generation >= expected_below
        || reference.path != super::publication::manifest_path(index_id, reference.blob.hash)
    {
        return Err(Status::data_loss(
            "index predecessor chain is non-canonical or cyclic",
        ));
    }
    Ok(())
}

fn retain_predecessor(
    retained_count: u32,
    retained_bytes: u64,
    reference: &ManifestReference,
    manifest: &IndexGenerationManifest,
    now: u64,
    config: IndexRuntimeConfig,
) -> bool {
    let within_count = retained_count < config.max_retained_generations();
    let within_age = now.saturating_sub(reference.published_at_unix_millis)
        < config
            .max_generation_age_hours()
            .saturating_mul(60 * 60 * 1_000);
    let within_bytes = retained_bytes
        .checked_add(manifest.authoritative_bytes)
        .is_some_and(|total| total <= config.max_retained_generation_bytes());
    within_count && within_age && within_bytes
}

fn delete_command(index_id: u64, version: VersionId, class: &str, path: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(class.as_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&version.0.to_be_bytes());
    format!(
        "index-v2-gc-{index_id}-{}",
        &hasher.finalize().to_hex().as_str()[..24]
    )
}

fn now_unix_millis() -> Result<u64, Status> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock predates the Unix epoch"))?
            .as_millis(),
    )
    .map_err(|_| Status::internal("system time exceeds u64 milliseconds"))
}

#[cfg(test)]
mod tests {
    use anvil_store::{BlobRef, PlacementLogId, SourceId, Version};
    use tonic::Code;

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexBarrier, IndexSourceCursor};
    use crate::index_runtime::generation::{IndexGenerationManifest, ManifestRun};

    fn config(count: u32, age: u64, bytes: u64) -> IndexRuntimeConfig {
        IndexRuntimeConfig::new(1, 1, 64 * 1024 * 1024, 1, count, age, bytes).unwrap()
    }

    fn manifest(generation: u64, bytes: u64, run_hash: [u8; 32]) -> IndexGenerationManifest {
        let barrier = IndexBarrier {
            fence: PlacementLogId { term: 1, index: 1 },
            atomic: AtomicProgramWatermark::new(None, None, 0),
            sources: [(
                anvil_consensus::NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [1; 32],
                    },
                    next_offset: 1,
                },
            )]
            .into_iter()
            .collect(),
        };
        IndexGenerationManifest::new(
            9,
            generation,
            1,
            anvil_index::IndexKind::Path,
            &barrier,
            vec![ManifestRun {
                sequence: generation,
                level: 0,
                root_path: super::super::publication::run_root_path(9, run_hash),
                root_blob: BlobRef {
                    hash: run_hash,
                    length: 10,
                },
                root_object_version: VersionId(generation),
                mutation_count: 1,
                live_document_count: 1,
                minimum_version: 1,
                maximum_version: 1,
                authoritative_bytes: bytes,
            }],
            None,
            1,
            0,
        )
        .unwrap()
    }

    #[test]
    fn first_count_age_or_byte_bound_stops_the_retained_prefix() {
        let candidate = manifest(2, 40, [2; 32]);
        let reference = ManifestReference {
            generation: 2,
            definition_version: 1,
            path: super::super::publication::manifest_path(9, [8; 32]),
            blob: BlobRef {
                hash: [8; 32],
                length: 10,
            },
            object_version: VersionId(2),
            published_at_unix_millis: 90_000_000,
        };
        assert!(!retain_predecessor(
            2,
            40,
            &reference,
            &candidate,
            100_000_000,
            config(2, 24, 100)
        ));
        assert!(!retain_predecessor(
            1,
            40,
            &reference,
            &candidate,
            200_000_001,
            config(3, 24, 100)
        ));
        assert!(!retain_predecessor(
            1,
            70,
            &reference,
            &candidate,
            100_000_000,
            config(3, 24, 100)
        ));
    }

    #[test]
    fn retained_run_identity_is_path_scoped_even_when_block_content_is_shared() {
        let retained = manifest(3, 100, [3; 32]);
        let obsolete = manifest(2, 100, [2; 32]);
        let shared_block_hash = [7; 32];
        let retained_block = super::super::publication::run_block_path(
            9,
            retained.runs[0].root_blob.hash,
            shared_block_hash,
        );
        let obsolete_block = super::super::publication::run_block_path(
            9,
            obsolete.runs[0].root_blob.hash,
            shared_block_hash,
        );
        let mut kept = RetainedArtifacts::default();
        kept.insert("retained-manifest", &retained);

        assert!(
            kept.run_hashes
                .contains(&run_hash_from_artifact_path(9, &retained_block).unwrap())
        );
        assert!(
            !kept
                .run_hashes
                .contains(&run_hash_from_artifact_path(9, &obsolete_block).unwrap())
        );
        assert_ne!(retained_block, obsolete_block);
    }

    #[test]
    fn repeated_collection_stops_at_a_proven_pruned_predecessor() {
        let referenced = VersionId(41);
        let tombstone = Version {
            id: VersionId(42),
            blob: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 100,
        };

        for _ in 0..2 {
            assert!(matches!(
                classify_absent_predecessor(referenced, Some(&tombstone)),
                Ok(LoadedPredecessor::PreviouslyPruned)
            ));
        }
    }

    #[test]
    fn absent_live_or_unmarked_predecessors_fail_closed() {
        let referenced = VersionId(41);
        let live = Version {
            id: VersionId(42),
            blob: Some(BlobRef {
                hash: [4; 32],
                length: 10,
            }),
            content_type: None,
            deleted: false,
            committed_at_unix_millis: 100,
        };
        let stale_tombstone = Version {
            id: referenced,
            blob: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 100,
        };

        for current in [Some(&live), Some(&stale_tombstone), None] {
            let error = match classify_absent_predecessor(referenced, current) {
                Ok(_) => panic!("unproven predecessor pruning must fail closed"),
                Err(error) => error,
            };
            assert_eq!(error.code(), Code::DataLoss);
        }
    }
}
