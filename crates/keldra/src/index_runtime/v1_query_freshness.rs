//! Public freshness evidence for a pinned format-v1 root vector.

use std::collections::BTreeMap;

use keldra_api::v1::{IndexFreshness, IndexSourceFreshness};
use keldra_index::v1::ProjectionPartitionIdentity;
use keldra_store::PlacementLogId;
use tonic::Status;

use super::{LocalIndexQueryRequest, PinnedRootVector};

pub(super) fn freshness(
    request: &LocalIndexQueryRequest,
    pinned: &PinnedRootVector,
    fence: PlacementLogId,
    atomic: u64,
    observed_source_next: impl Fn(&ProjectionPartitionIdentity) -> Option<u64>,
) -> Result<IndexFreshness, Status> {
    struct SourceObservation {
        source_epoch: [u8; 32],
        indexed_next_offset: u64,
        observed_next_offset: Option<u64>,
        complete: bool,
    }

    let mut observations = BTreeMap::<u64, SourceObservation>::new();
    for root in &pinned.roots {
        let observed = observed_source_next(&root.partition);
        let entry = observations
            .entry(root.partition.source_node)
            .or_insert(SourceObservation {
                source_epoch: root.partition.source_epoch,
                indexed_next_offset: root.root.next_offset,
                observed_next_offset: observed,
                complete: observed.is_some(),
            });
        if entry.source_epoch != root.partition.source_epoch {
            return Err(Status::data_loss(
                "v1 query root vector contains conflicting source epochs",
            ));
        }
        entry.indexed_next_offset = entry.indexed_next_offset.max(root.root.next_offset);
        entry.complete &= observed.is_some();
        if let Some(observed) = observed {
            entry.observed_next_offset =
                Some(entry.observed_next_offset.unwrap_or_default().max(observed));
        }
    }
    let sources = observations
        .into_iter()
        .map(|(node_id, observation)| {
            // The immutable root is itself durable evidence that the source
            // reached `indexed_next_offset`. A cached monitor observation can
            // be older, but must never make that committed view look ahead of
            // its source. Missing observations remain explicitly unavailable
            // instead of manufacturing a zero-lag claim.
            let observed_next = observation.complete.then(|| {
                observation
                    .observed_next_offset
                    .unwrap_or_default()
                    .max(observation.indexed_next_offset)
            });
            IndexSourceFreshness {
                node_id,
                source_epoch: observation.source_epoch.to_vec(),
                indexed_next_offset: observation.indexed_next_offset,
                observed_tail: observed_next.and_then(|next| next.checked_sub(1)),
                lag_hint: observed_next.map_or(0, |next| {
                    next.saturating_sub(observation.indexed_next_offset)
                }),
            }
        })
        .collect();
    Ok(IndexFreshness {
        // This is the common atomic-program cut, not a general publication or
        // source generation. Zero is therefore correct for ordinary writes.
        commit_revision: atomic,
        published_at: None,
        sources,
        initial_build_complete: true,
        rebuilding: false,
        authorization_revision: request.authorization_revision,
        // The public service binds the admitted custom-realm evidence after
        // local or routed execution. The physical runtime cannot manufacture
        // that logical authorization claim.
        result_authorization: None,
        placement_term: fence.term,
        placement_index: fence.index,
        index_id: request.definition.index_id,
        definition_version: request.definition.version,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use keldra_index::v1::{
        PinnedPartitionQueryRoot, ProjectionFamilyPartitionDirectory, ProjectionQueryStreamRoot,
        QueryCommonCut, QueryRootCutProof,
    };

    use super::*;
    use crate::index_runtime::v1_query_runtime::handoff_lineage;
    use crate::index_service::{
        CandidateVisibilityEvidence, IndexCandidateIdentity, IndexCandidateVisibility,
    };

    fn partition(producer: u64, placement_index: u64) -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 7, [2; 32], producer, 3, placement_index).unwrap()
    }

    fn root(
        partition: ProjectionPartitionIdentity,
        next_offset: u64,
        through_atomic_position: u64,
    ) -> PinnedPartitionQueryRoot {
        let root = ProjectionQueryStreamRoot {
            stream_root_hash: [partition.producer_node as u8; 32],
            stream_root_encoded_bytes: 1,
            run_count: 1,
            first_sequence: 1,
            last_sequence: 1,
            source_start_offset: 0,
            next_offset,
            through_atomic_position,
        };
        PinnedPartitionQueryRoot {
            partition,
            physical_catalog_generation: [3; 32],
            root,
            cut_proof: QueryRootCutProof {
                common_cut: QueryCommonCut {
                    through_atomic_position,
                },
                selected_stream_root_hash: root.stream_root_hash,
                next_newer_through_atomic_position: None,
            },
            handoff_lineage_id: handoff_lineage(partition),
        }
    }

    fn pinned(atomic: u64, roots: Vec<PinnedPartitionQueryRoot>) -> PinnedRootVector {
        let cut = QueryCommonCut {
            through_atomic_position: atomic,
        };
        let identity = keldra_index::v1::query_snapshot_identity(cut, &roots).unwrap();
        PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity,
            cut,
            realtime_runs: Vec::new(),
            realtime_evidence: Vec::new(),
            overlay_generation_hashes: vec![None; roots.len()],
            generation_hashes: roots
                .iter()
                .map(|root| [root.partition.producer_node as u8; 32])
                .collect(),
            roots,
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        }
    }

    struct UnusedVisibility;

    #[tonic::async_trait]
    impl IndexCandidateVisibility for UnusedVisibility {
        async fn evaluate(
            &self,
            _: &[IndexCandidateIdentity],
        ) -> Result<CandidateVisibilityEvidence, Status> {
            unreachable!("freshness construction does not evaluate candidates")
        }
    }

    fn request() -> LocalIndexQueryRequest {
        LocalIndexQueryRequest {
            storage_tenant: "tenant".into(),
            tenant_id: 7,
            bucket_id: 9,
            definition: keldra_api::v1::IndexDefinition {
                bucket: "bucket".into(),
                index_id: 11,
                version: 13,
                ..Default::default()
            },
            query: Default::default(),
            limit: 100,
            resume: None,
            candidate_visibility: Arc::new(UnusedVisibility),
            authorization_revision: 19,
            required_freshness: None,
            required_visibility: Vec::new(),
            deadline: tokio::time::Instant::now(),
        }
    }

    #[test]
    fn reports_the_background_monitors_bucket_routed_lag() {
        let first = partition(4, 5);
        let second = partition(6, 7);
        let pinned = pinned(0, vec![root(first, 8, 0), root(second, 12, 0)]);
        let observed = BTreeMap::from([(first, 20), (second, 20)]);

        let freshness = freshness(
            &request(),
            &pinned,
            PlacementLogId { term: 3, index: 7 },
            0,
            |partition| observed.get(partition).copied(),
        )
        .unwrap();

        assert_eq!(
            freshness.commit_revision, 0,
            "ordinary writes retain the genesis atomic cut"
        );
        assert_eq!(freshness.sources.len(), 1);
        assert_eq!(freshness.sources[0].indexed_next_offset, 12);
        assert_eq!(freshness.sources[0].observed_tail, Some(19));
        assert_eq!(freshness.sources[0].lag_hint, 8);
    }

    #[test]
    fn does_not_manufacture_zero_lag_when_an_observation_is_missing() {
        let first = partition(4, 5);
        let second = partition(6, 7);
        let pinned = pinned(9, vec![root(first, 8, 9), root(second, 12, 9)]);

        let freshness = freshness(
            &request(),
            &pinned,
            PlacementLogId { term: 3, index: 7 },
            9,
            |partition| (partition == &first).then_some(20),
        )
        .unwrap();

        assert_eq!(freshness.sources.len(), 1);
        assert_eq!(freshness.sources[0].observed_tail, None);
        assert_eq!(freshness.sources[0].lag_hint, 0);
    }

    #[test]
    fn stale_monitor_observation_never_precedes_the_durable_root() {
        let selected = partition(4, 5);
        let pinned = pinned(9, vec![root(selected, 12, 9)]);

        let freshness = freshness(
            &request(),
            &pinned,
            PlacementLogId { term: 3, index: 7 },
            9,
            |_| Some(7),
        )
        .unwrap();

        assert_eq!(freshness.sources[0].indexed_next_offset, 12);
        assert_eq!(freshness.sources[0].observed_tail, Some(11));
        assert_eq!(freshness.sources[0].lag_hint, 0);
    }
}
