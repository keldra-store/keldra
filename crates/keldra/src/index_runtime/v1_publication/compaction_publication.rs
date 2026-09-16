//! Publication of completed maintenance while a source partition is idle.

use keldra_index::v1::{
    ProjectionCurrent, ProjectionGeneration, encode_projection_current,
    encode_projection_generation, projection_component_page_path, projection_generation_path,
};

use super::*;

impl V1ProjectionPublisher {
    /// Build a successor whose source cut is unchanged and whose only change
    /// is a completed, already-durable compaction. This avoids inventing a
    /// synthetic journal row or an empty query run merely to publish it.
    pub(crate) fn prepare_compaction_publication(
        &self,
        partition: ProjectionPartitionIdentity,
        current: &LoadedV1ProjectionGeneration,
        compaction: V1CompactionBase,
        artifacts: V1CompactionArtifacts,
    ) -> Result<PendingV1Publication, Status> {
        let generation = compaction_successor_generation(current, &compaction.predecessor)?;
        let encoded = encode_projection_generation(&generation).map_err(index_status)?;
        let current_record =
            ProjectionCurrent::new(encoded.hash, &generation).map_err(index_status)?;
        let current_bytes = encode_projection_current(current_record).map_err(index_status)?;
        let mut immutable = BTreeMap::new();
        for page in encoded.component_directory.pages {
            insert_artifact(
                &mut immutable,
                projection_component_page_path(partition, page.hash),
                keldra_index::v1::ProjectionArtifactKind::ComponentPage,
                page.hash,
                page.bytes,
            )?;
        }
        insert_artifact(
            &mut immutable,
            projection_generation_path(partition, encoded.hash),
            keldra_index::v1::ProjectionArtifactKind::Generation,
            encoded.hash,
            encoded.bytes,
        )?;
        Ok(PendingV1Publication {
            plan: AtomicPublicationPlan {
                immutable: immutable.into_values().collect(),
                current_bytes,
                current: current_record,
                generation,
                sealed_bytes: 0,
                source_positions: 0,
                state_updates: Vec::new(),
                _publication_credits: None,
            },
            expected_current_version: Some(current.current_object_version),
            previous_generation_hash: Some(current.current.generation_hash),
            checkpointed_source_positions: 0,
            checkpointed_source_payload_bytes: 0,
            _compaction: Some(artifacts),
        })
    }
}

pub(super) fn compaction_successor_generation(
    current: &LoadedV1ProjectionGeneration,
    compacted: &ProjectionGeneration,
) -> Result<ProjectionGeneration, Status> {
    let predecessor = &current.generation;
    if compacted.partition != predecessor.partition
        || compacted.physical_catalog_generation != predecessor.physical_catalog_generation
        || compacted.revision != predecessor.revision
        || compacted.next_offset != predecessor.next_offset
        || compacted.through_atomic_position != predecessor.through_atomic_position
        || compacted.inherited_partitions != predecessor.inherited_partitions
        || compacted.previous_generation_hash != predecessor.previous_generation_hash
    {
        return Err(Status::data_loss(
            "v1 compaction changed non-compactable generation identity",
        ));
    }
    predecessor
        .advance(
            current.current.generation_hash,
            predecessor.physical_catalog_generation,
            predecessor.next_offset,
            predecessor.through_atomic_position,
            compacted.roots.clone(),
        )
        .and_then(|generation| generation.with_query_stream_root(compacted.query_stream_root))
        .map_err(index_status)
}
