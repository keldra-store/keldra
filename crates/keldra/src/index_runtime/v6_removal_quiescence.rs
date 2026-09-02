//! Pre-cutover proof that a departing source incarnation has a durable v6 tail.

use std::collections::BTreeMap;

use keldra_index::v6::{
    ProjectionCurrent, ProjectionFamilyPartitionDirectory, ProjectionPartitionIdentity,
};
use keldra_store::SourceId;
use tonic::Status;

/// Evidence a membership handoff must obtain before its old source journal is
/// allowed to disappear. This is deliberately a pure proof: membership owns
/// when to call it while the departing node and journal are still available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V6RemovalQuiescenceProof {
    pub(crate) source: SourceId,
    pub(crate) settled_next_offset: u64,
    pub(crate) settled_atomic_position: u64,
    pub(crate) covered_partitions: Vec<ProjectionPartitionIdentity>,
}

impl V6RemovalQuiescenceProof {
    pub(crate) fn prove(
        source: SourceId,
        settled_next_offset: u64,
        settled_atomic_position: u64,
        directory: &ProjectionFamilyPartitionDirectory,
        currents: &BTreeMap<ProjectionPartitionIdentity, ProjectionCurrent>,
    ) -> Result<Self, Status> {
        directory.validate().map_err(index_status)?;
        if settled_next_offset == 0 {
            return Err(Status::failed_precondition(
                "v6 removal quiescence requires a settled source tail",
            ));
        }
        let source_node = u64::from(source.node_id);
        let mut covered_partitions = Vec::new();
        for entry in directory.entries.iter().filter(|entry| {
            entry.partition.source_node == source_node
                && entry.partition.source_epoch == source.source_epoch
        }) {
            let current = currents.get(&entry.partition).ok_or_else(|| {
                Status::failed_precondition(
                    "v6 removal quiescence lacks a required partition current",
                )
            })?;
            if current.partition != entry.partition
                || current.next_offset < settled_next_offset
                || current.through_atomic_position < settled_atomic_position
            {
                return Err(Status::failed_precondition(
                    "v6 partition has not reached the settled removal cut",
                ));
            }
            covered_partitions.push(entry.partition);
        }
        if covered_partitions.is_empty() {
            return Err(Status::failed_precondition(
                "v6 removal quiescence found no directory partition for source",
            ));
        }
        Ok(Self {
            source,
            settled_next_offset,
            settled_atomic_position,
            covered_partitions,
        })
    }
}

fn index_status(error: keldra_index::IndexError) -> Status {
    Status::data_loss(format!("invalid v6 removal directory: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v6::{ProjectionPartitionDirectoryEntry, ProjectionPartitionLifecycle};

    fn source() -> SourceId {
        SourceId {
            node_id: 7,
            source_epoch: [7; 32],
        }
    }

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 7, [7; 32], 7, 2, 3).unwrap()
    }

    fn directory() -> ProjectionFamilyPartitionDirectory {
        ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![ProjectionPartitionDirectoryEntry {
                partition: partition(),
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            }],
        }
    }

    fn current(next_offset: u64, atomic: u64) -> ProjectionCurrent {
        ProjectionCurrent {
            partition: partition(),
            physical_catalog_generation: [2; 32],
            generation_hash: [3; 32],
            generation_revision: 1,
            next_offset,
            through_atomic_position: atomic,
        }
    }

    #[test]
    fn caught_up_source_proves_its_exact_final_cut() {
        let currents = BTreeMap::from([(partition(), current(12, 9))]);
        let proof =
            V6RemovalQuiescenceProof::prove(source(), 12, 9, &directory(), &currents).unwrap();
        assert_eq!(proof.covered_partitions, vec![partition()]);
    }

    #[test]
    fn lagging_partition_refuses_removal() {
        let currents = BTreeMap::from([(partition(), current(11, 9))]);
        assert!(V6RemovalQuiescenceProof::prove(source(), 12, 9, &directory(), &currents).is_err());
    }
}
