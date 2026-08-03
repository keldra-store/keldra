//! Authoritative current-object snapshots consumed by index engines.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use anvil_api::v1::IndexSpecification;
use anvil_api::v1::index_specification::Specification;
use anvil_store::{LocalChange, ObjectKey};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_service::{StoredIndexDefinition, path_matches_prefix};

use super::engine::IndexBuildObject;
use super::events::{IndexBarrier, IndexJournalBatch};
use super::scanner::ClusterIndexScanner;

#[derive(Clone, Default)]
pub(crate) struct IndexObjectSnapshot {
    objects: BTreeMap<String, IndexBuildObject>,
}

impl IndexObjectSnapshot {
    pub(crate) async fn initial(
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        barrier: &IndexBarrier,
        scanner: &ClusterIndexScanner,
        reader: &ClusterObjectReader,
    ) -> Result<Self, Status> {
        let specification = definition.specification()?;
        let heads = scanner
            .scan(IndexHeadScanScope::SourceObjects {
                tenant_id,
                bucket_id,
                path_prefix: definition.path_prefix.clone(),
            })
            .await?;
        let mut snapshot = Self::default();
        for head in heads {
            match require_visible_initial(
                load_current(
                    definition,
                    &specification,
                    tenant_id,
                    bucket_id,
                    &head.exact_path,
                    barrier,
                    reader,
                )
                .await?,
            )? {
                Some(object) => {
                    snapshot.objects.insert(object.path.clone(), object);
                }
                None => {}
            }
        }
        Ok(snapshot)
    }

    pub(crate) async fn apply(
        &mut self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        batch: &IndexJournalBatch,
        reader: &ClusterObjectReader,
    ) -> Result<bool, Status> {
        let specification = definition.specification()?;
        let mut paths = BTreeSet::new();
        for change in &batch.changes {
            let LocalChange::ObjectHead(head) = &change.change else {
                continue;
            };
            if head.tenant_id == tenant_id
                && head.bucket_id == bucket_id
                && path_matches_prefix(&head.exact_path, &definition.path_prefix)
                && !contains_reserved_segment(&head.exact_path)
            {
                paths.insert(head.exact_path.clone());
            }
        }
        let mut loaded = Vec::with_capacity(paths.len());
        for path in paths {
            let current = load_current(
                definition,
                &specification,
                tenant_id,
                bucket_id,
                &path,
                &batch.through,
                reader,
            )
            .await?;
            if matches!(current, LoadedCurrent::Deferred) {
                return Err(Status::unavailable(
                    "an atomic program advanced during the index update; retry the batch",
                ));
            }
            loaded.push((path, current));
        }
        self.apply_loaded(loaded)
    }

    fn apply_loaded(&mut self, loaded: Vec<(String, LoadedCurrent)>) -> Result<bool, Status> {
        if loaded
            .iter()
            .any(|(_, current)| matches!(current, LoadedCurrent::Deferred))
        {
            return Err(Status::unavailable(
                "an atomic program advanced during the index update; retry the batch",
            ));
        }

        let mut changed = false;
        for (path, current) in loaded {
            match current {
                LoadedCurrent::Present(object) => {
                    changed |= self
                        .objects
                        .get(&path)
                        .is_none_or(|old| old.version != object.version);
                    self.objects.insert(path, object);
                }
                LoadedCurrent::Absent => changed |= self.objects.remove(&path).is_some(),
                LoadedCurrent::Deferred => unreachable!("deferred values were rejected above"),
            }
        }
        Ok(changed)
    }

    pub(crate) fn values(&self) -> Vec<IndexBuildObject> {
        self.objects.values().cloned().collect()
    }
}

enum LoadedCurrent {
    Present(IndexBuildObject),
    Absent,
    Deferred,
}

fn require_visible_initial(loaded: LoadedCurrent) -> Result<Option<IndexBuildObject>, Status> {
    match loaded {
        LoadedCurrent::Present(object) => Ok(Some(object)),
        LoadedCurrent::Absent => Ok(None),
        LoadedCurrent::Deferred => Err(Status::unavailable(
            "an atomic program advanced during the index snapshot; retry the scan",
        )),
    }
}

async fn load_current(
    definition: &StoredIndexDefinition,
    specification: &IndexSpecification,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
    barrier: &IndexBarrier,
    reader: &ClusterObjectReader,
) -> Result<LoadedCurrent, Status> {
    if !path_matches_prefix(path, &definition.path_prefix) || contains_reserved_segment(path) {
        return Ok(LoadedCurrent::Absent);
    }
    let key = ObjectKey::new(&definition.tenant, &definition.bucket, path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let Some(opened) = reader.open_stable(&key, tenant_id, bucket_id, None).await? else {
        return Ok(LoadedCurrent::Absent);
    };
    if !barrier.atomic.permits(opened.program_commit_cursor) {
        return Ok(LoadedCurrent::Deferred);
    }
    if opened.version.deleted {
        return Ok(LoadedCurrent::Absent);
    }
    if definition
        .content_type
        .as_ref()
        .is_some_and(|required| opened.version.content_type.as_ref() != Some(required))
    {
        return Ok(LoadedCurrent::Absent);
    }
    let blob =
        opened.version.blob.clone().ok_or_else(|| {
            Status::data_loss("live index source object has no payload reference")
        })?;
    let payload = if specification_requires_payload(specification) {
        let Some(mut payload) = opened.payload else {
            return Err(Status::data_loss(
                "live index source object has no readable payload",
            ));
        };
        let mut bytes = Vec::new();
        payload
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read index source object: {error}")))?;
        Some(bytes)
    } else {
        None
    };
    Ok(LoadedCurrent::Present(IndexBuildObject {
        path: path.to_owned(),
        version: opened.version.id.0,
        content_type: opened.version.content_type,
        content_hash: blob.hash,
        content_length: blob.length,
        committed_at_unix_millis: opened.version.committed_at_unix_millis,
        payload,
    }))
}

fn specification_requires_payload(specification: &IndexSpecification) -> bool {
    !matches!(
        specification.specification,
        Some(Specification::Path(_) | Specification::MetadataFilter(_))
    )
}

fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_anvil")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(path: &str, version: u64) -> IndexBuildObject {
        IndexBuildObject {
            path: path.into(),
            version,
            content_type: None,
            content_hash: [0; 32],
            content_length: 0,
            committed_at_unix_millis: 0,
            payload: None,
        }
    }

    #[test]
    fn deferred_program_head_aborts_an_initial_snapshot() {
        let error = require_visible_initial(LoadedCurrent::Deferred).unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn deferred_program_head_aborts_the_whole_incremental_batch() {
        let mut snapshot = IndexObjectSnapshot::default();
        snapshot.objects.insert("a".into(), object("a", 1));
        snapshot.objects.insert("b".into(), object("b", 1));

        let error = snapshot
            .apply_loaded(vec![
                ("a".into(), LoadedCurrent::Present(object("a", 2))),
                ("b".into(), LoadedCurrent::Deferred),
            ])
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(snapshot.objects["a"].version, 1);
        assert_eq!(snapshot.objects["b"].version, 1);
    }

    #[test]
    fn reserved_namespace_is_segment_aware() {
        assert!(contains_reserved_segment("a/_anvil/value"));
        assert!(contains_reserved_segment("_anvil/value"));
        assert!(!contains_reserved_segment("a/not_anvil/value"));
    }
}
