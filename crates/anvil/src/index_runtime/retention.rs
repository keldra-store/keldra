//! Configured count/age/byte retention for obsolete immutable generations.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_store::VersionId;
use tonic::Status;

use crate::cluster_peer::IndexHeadScanScope;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::StoredIndexDefinition;

use super::publication::{IndexArtifactDelete, IndexArtifactRouter, current_path};
use super::scanner::ClusterIndexScanner;

#[derive(Clone)]
pub(crate) struct IndexGenerationRetention {
    scanner: ClusterIndexScanner,
    artifacts: IndexArtifactRouter,
    config: IndexRuntimeConfig,
}

impl IndexGenerationRetention {
    pub(crate) fn new(
        scanner: ClusterIndexScanner,
        artifacts: IndexArtifactRouter,
        config: IndexRuntimeConfig,
    ) -> Self {
        Self {
            scanner,
            artifacts,
            config,
        }
    }

    pub(crate) async fn collect(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current_generation: u64,
    ) -> Result<u64, Status> {
        let heads = self
            .scanner
            .scan(IndexHeadScanScope::Generation {
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
            })
            .await?;
        let mut generations = BTreeMap::<u64, RetainedGeneration>::new();
        let current_pointer_path = current_path(definition.index_id);
        let mut superseded_current_pointers = Vec::new();
        for head in heads {
            if head.exact_path == current_pointer_path {
                superseded_current_pointers.extend(superseded_current_pointer_artifacts(&head));
                continue;
            }
            if head.head.deleted || head.version.deleted {
                continue;
            }
            let Some(generation) = generation_from_path(definition.index_id, &head.exact_path)
            else {
                continue;
            };
            let bytes = head.version.blob.as_ref().map_or(0, |blob| blob.length);
            let retained = generations.entry(generation).or_default();
            retained.bytes = retained.bytes.saturating_add(bytes);
            retained.newest_commit_millis = retained
                .newest_commit_millis
                .max(head.version.committed_at_unix_millis);
            retained.artifacts.push(RetainedArtifact {
                path: head.exact_path,
                version: head.version.id,
            });
        }
        let selected = select_obsolete(
            &generations,
            current_generation,
            self.config,
            now_unix_millis()?,
        );
        let mut deleted = 0_u64;
        for generation in selected {
            let Some(retained) = generations.get(&generation) else {
                continue;
            };
            for artifact in &retained.artifacts {
                self.artifacts
                    .delete(IndexArtifactDelete {
                        storage_tenant: definition.tenant.clone(),
                        bucket: definition.bucket.clone(),
                        tenant_id,
                        bucket_id,
                        index_id: definition.index_id,
                        exact_path: artifact.path.clone(),
                        expected_version: artifact.version,
                        command_id: delete_command(definition.index_id, generation, &artifact.path),
                    })
                    .await?;
                deleted = deleted.saturating_add(1);
            }
        }
        // Versioned buckets retain every replaced `current` pointer. Those
        // historical pointer blobs are never queryable: a query binds the
        // current pointer once and then opens the immutable generation named
        // by it. Retire them through the same exact-version object path used
        // for every other versioned object, leaving the live pointer intact.
        for artifact in superseded_current_pointers {
            self.artifacts
                .delete(IndexArtifactDelete {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    index_id: definition.index_id,
                    exact_path: artifact.path,
                    expected_version: artifact.version,
                    command_id: current_pointer_delete_command(
                        definition.index_id,
                        artifact.version,
                    ),
                })
                .await?;
            deleted = deleted.saturating_add(1);
        }
        Ok(deleted)
    }
}

#[derive(Default)]
struct RetainedGeneration {
    bytes: u64,
    newest_commit_millis: u64,
    artifacts: Vec<RetainedArtifact>,
}

struct RetainedArtifact {
    path: String,
    version: VersionId,
}

fn superseded_current_pointer_artifacts(
    head: &crate::cluster_peer::IndexCurrentHead,
) -> Vec<RetainedArtifact> {
    head.versions
        .iter()
        .filter(|version| {
            version.id != head.head.version && !version.deleted && version.blob.is_some()
        })
        .map(|version| RetainedArtifact {
            path: head.exact_path.clone(),
            version: version.id,
        })
        .collect()
}

fn select_obsolete(
    generations: &BTreeMap<u64, RetainedGeneration>,
    current: u64,
    config: IndexRuntimeConfig,
    now_millis: u64,
) -> BTreeSet<u64> {
    let mut selected = BTreeSet::new();
    let age_millis = config
        .max_generation_age_hours()
        .saturating_mul(60 * 60 * 1_000);
    for (generation, retained) in generations {
        if *generation != current
            && now_millis.saturating_sub(retained.newest_commit_millis) >= age_millis
        {
            selected.insert(*generation);
        }
    }

    let max_count = config.max_retained_generations() as usize;
    let mut remaining_count = generations.len().saturating_sub(selected.len());
    for generation in generations.keys() {
        if remaining_count <= max_count {
            break;
        }
        if *generation != current && selected.insert(*generation) {
            remaining_count -= 1;
        }
    }

    let mut remaining_bytes = generations
        .iter()
        .filter(|(generation, _)| !selected.contains(generation))
        .fold(0_u64, |total, (_, retained)| {
            total.saturating_add(retained.bytes)
        });
    for (generation, retained) in generations {
        if remaining_bytes <= config.max_retained_generation_bytes() {
            break;
        }
        if *generation != current && selected.insert(*generation) {
            remaining_bytes = remaining_bytes.saturating_sub(retained.bytes);
        }
    }
    selected
}

fn generation_from_path(index_id: u64, path: &str) -> Option<u64> {
    let mut parts = path.split('/');
    if parts.next()? != "_anvil"
        || parts.next()? != "indexes"
        || parts.next()?.parse::<u64>().ok()? != index_id
        || parts.next()? != "generations"
    {
        return None;
    }
    let generation = parts.next()?.parse::<u64>().ok()?;
    (generation != 0).then_some(generation)
}

fn delete_command(index_id: u64, generation: u64, path: &str) -> String {
    let digest = blake3::hash(path.as_bytes());
    format!(
        "index-gc-{index_id}-{generation}-{}",
        &digest.to_hex().as_str()[..16]
    )
}

fn current_pointer_delete_command(index_id: u64, version: VersionId) -> String {
    format!("index-current-gc-{index_id}-{}", version.0)
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
    use anvil_store::{BlobRef, Head, Version};

    use super::*;

    fn retained(bytes: u64, committed: u64) -> RetainedGeneration {
        RetainedGeneration {
            bytes,
            newest_commit_millis: committed,
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn first_count_age_or_bytes_limit_selects_oldest_but_never_current() {
        let values = BTreeMap::from([
            (1, retained(40, 1)),
            (2, retained(40, 90_000_000)),
            (3, retained(40, 90_000_000)),
        ]);
        let config = IndexRuntimeConfig::new(1, 1, 2, 24, 70).unwrap();
        let selected = select_obsolete(&values, 3, config, 100_000_000);
        assert!(selected.contains(&1));
        assert!(selected.contains(&2));
        assert!(!selected.contains(&3));
    }

    #[test]
    fn only_historical_live_current_pointer_blobs_are_retired() {
        let version = |id: u64, blob: bool, deleted: bool| Version {
            id: VersionId(id),
            blob: blob.then_some(BlobRef {
                hash: [id as u8; 32],
                length: 1,
            }),
            content_type: None,
            deleted,
            committed_at_unix_millis: id,
        };
        let head = crate::cluster_peer::IndexCurrentHead {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: current_path(3),
            head: Head {
                version: VersionId(4),
                deleted: false,
                mutation_stamp: None,
            },
            version: version(4, true, false),
            versions: vec![
                version(1, true, false),
                version(2, false, true),
                version(3, false, false),
                version(4, true, false),
            ],
        };
        let selected = superseded_current_pointer_artifacts(&head);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].path, current_path(3));
        assert_eq!(selected[0].version, VersionId(1));
    }
}
